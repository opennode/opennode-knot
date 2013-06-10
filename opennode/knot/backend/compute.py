from grokcore.component import context, baseclass
from grokcore.component import implements
from logging import DEBUG, WARNING, ERROR
from twisted.internet import defer
from twisted.python import log
from uuid import uuid5, NAMESPACE_DNS
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility

import netaddr

from opennode.knot.backend.operation import IDeployVM
from opennode.knot.backend.operation import IDestroyVM
from opennode.knot.backend.operation import IListVMS
from opennode.knot.backend.operation import IMigrateVM
from opennode.knot.backend.operation import IRebootVM
from opennode.knot.backend.operation import IResumeVM
from opennode.knot.backend.operation import IShutdownVM
from opennode.knot.backend.operation import IStartVM
from opennode.knot.backend.operation import ISuspendVM
from opennode.knot.backend.operation import IUndeployVM
from opennode.knot.backend.operation import OperationRemoteError
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.model.compute import ICompute, Compute, IVirtualCompute
from opennode.knot.model.compute import IUndeployed, IDeployed, IDeploying
from opennode.knot.model.compute import IManageable
from opennode.knot.model.template import ITemplate
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer

from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.cmd.base import Cmd
from opennode.oms.endpoint.ssh.cmd.directives import command
from opennode.oms.endpoint.ssh.cmdline import ICmdArgumentsSyntax
from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.endpoint.ssh.cmd.security import require_admins_only
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.log import UserLogger
from opennode.oms.model.form import alsoProvides
from opennode.oms.model.form import noLongerProvides
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.hooks import PreValidateHookMixin
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.model.traversal import canonical_path, traverse1
from opennode.oms.zodb import db


def any_stack_installed(context):
    return IManageable.providedBy(context)


def format_error(e):
    return (": ".join(msg for msg in e.args if isinstance(msg, str) and not msg.startswith('  File "/')))


@defer.inlineCallbacks
def register_machine(host, mgt_stack=IManageable):

    @db.ro_transact
    def check():
        machines = db.get_root()['oms_root']['machines']
        machine = follow_symlinks(machines['by-name'][host])
        if not mgt_stack.providedBy(machine):
            return
        return machine

    @db.transact
    def update():
        machines = db.get_root()['oms_root']['machines']
        machine = Compute(unicode(host), u'active', mgt_stack=mgt_stack)
        machine.__name__ = str(uuid5(NAMESPACE_DNS, host))
        machines.add(machine)
        return machine.__name__

    if not (yield check()):
        defer.returnValue((yield update()))


def find_compute_v12n_container(compute, backend):
    for v12nc in compute:
        if IVirtualizationContainer.providedBy(v12nc) and v12nc.backend == backend:
            return v12nc


class ComputeAction(Action, PreValidateHookMixin):
    context(ICompute)
    baseclass()

    _lock_registry = {}
    _do_not_enqueue = False

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context.__parent__.__parent__,))

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, self.context)

    @property
    def lock_keys(self):
        """ Returns list of object-related 'hashes' to lock against. Overload in derived classes
        to add more objects """
        return (canonical_path(self.context),)

    def locked(self):
        return any(key in self._lock_registry for key in self.lock_keys)

    def acquire(self):
        d = defer.Deferred()
        log.msg('%s acquiring locks for: %s' % (self, self.lock_keys),
                system='compute-action', logLevel=DEBUG)
        self._used_lock_keys = []
        for key in self.lock_keys:
            self._lock_registry[key] = (d, self)
            self._used_lock_keys.append(key)
        return d

    def reacquire(self):
        """ Add more locks for the currently executed action. Meant to be called from _execute to add
        locks for objects that are evaluated in _execute() and added to _lock_keys.
        Returns a deferred list of all deferreds that already have locks on, or None if no other action
        locks have been found"""
        d = self._lock_registry[self.lock_keys[0]][0]
        deferred_list = []

        log.msg('%s adding locks for: %s' % (self, self.lock_keys),
                system='compute-action', logLevel=DEBUG)

        for key in self.lock_keys:
            if key not in self._lock_registry:
                self._lock_registry[key] = (d, self)
                self._used_lock_keys.append(key)
            elif self._lock_registry[key][0] is not d:
                dother, actionother = self._lock_registry[key]
                log.msg('Another action %s locked %s... %s will wait until it finishes'
                    % (actionother, key, self), system='compute-action')
                deferred_list.append(dother)

        if len(deferred_list) > 0:
            return defer.DeferredList(deferred_list, consumeErrors=True)

    @defer.inlineCallbacks
    def reacquire_until_clear(self):
        while True:
            dl = self.reacquire()

            if dl is None:
                break

            log.msg('%s is waiting for other actions locking additional objects (%s)...'
                    % (self, self._additional_keys), system='compute-action')

            yield dl  # wait for any actions locking our targets

            log.msg('%s will attempt to reacquire locks again (%s)...'
                    % (self, self._additional_keys), system='compute-action')

    def _release_and_fire_next_now(self):
        ld = None
        for key in self._used_lock_keys:
            if not ld:
                ld, _ = self._lock_registry[key]
            del self._lock_registry[key]
        del self._used_lock_keys
        ld.callback(None)

    def release(self, d):
        d.addBoth(lambda r: self._release_and_fire_next_now())
        return d

    def _action_log(self, cmd, msg, **kwargs):
        if not kwargs.get('system'):
            kwargs['system'] ='compute-action'
        log.msg(msg, **kwargs)
        cmd.write('%s\n' % msg)

    @defer.inlineCallbacks
    def add_log_event(self, cmd, msg, *args, **kwargs):
        self._action_log(cmd, msg)
        owner = yield db.get(self.context, '__owner__')
        ulog = UserLogger(principal=cmd.protocol.interaction.participations[0].principal,
                          subject=self.context, owner=owner)
        ulog.log(msg, *args, **kwargs)

    @defer.inlineCallbacks
    def handle_error(self, f, cmd):
        msg = '%s: "%s" executing "%s"' % (type(f.value).__name__, f.value, self)
        yield self.add_log_event(cmd, msg)
        log.err(f, system='compute-action')
        defer.returnValue(f)

    def handle_action_done(self, r, cmd):
        return self.add_log_event(cmd, '%s(%s) finished' % (self, self.context))

    def execute(self, cmd, args):
        if self.locked():
            for key in self.lock_keys:
                data = self._lock_registry.get(key)
                if data is not None:
                    break
            else:
                self._action_log(cmd,
                                 'CONCURRENCY BUG: locked() returned True, but now it is False! %s' % (self))
                raise KeyError(self.lock_keys)

            ld, lock_action = data

            if self._do_not_enqueue:
                self._action_log(cmd, '%s: %s is locked by %s. Skipping due to no-enqueue feature of '
                                 'the action.' % (self, self.context, lock_action))
                return ld

            self._action_log(cmd, '%s: %s is locked by %s. Scheduling to run after finish of previous action'
                             % (self, self.context, lock_action))

            def execute_on_unlock(r):
                self._action_log(cmd, '%s: %s is unlocked. Executing now' % (self, self.context),
                                 logLevel=DEBUG)
                # XXX: must be self.execute(), not self._execute(): next action must lock all its objects
                self.execute(cmd, args)

            ld.addBoth(execute_on_unlock)
            return ld

        ld = self.acquire()

        @defer.inlineCallbacks
        def cancel_action(e, cmd):
            e.trap(Exception)
            msg = 'Canceled executing "%s" due to validate_hook failure' % self
            yield self.add_log_event(cmd, msg)

        try:
            principal = cmd.protocol.interaction.participations[0].principal
            owner = getUtility(IAuthentication).getPrincipal(self.context.__owner__)
            d = defer.maybeDeferred(self.validate_hook, principal if principal.id != 'root' else owner)
        except Exception:
            ld.addErrback(cancel_action, cmd)
            self._release_and_fire_next_now()
            return ld

        d.addCallback(lambda r: self._execute(cmd, args))
        d.addErrback(self.handle_error, cmd)
        d.addCallback(self.handle_action_done, cmd)
        return self.release(d)

    def _execute(self, cmd, args):
        """ Must be overloaded by child classes """
        raise NotImplementedError()


class VComputeAction(ComputeAction):
    """Common code for virtual compute actions."""
    context(IVirtualCompute)
    baseclass()

    @property
    def lock_keys(self):
        return [canonical_path(self.context),
                canonical_path(self.context.__parent__),
                canonical_path(self.context.__parent__.__parent__)]

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        action_name = getattr(self, 'action_name', self._name + "ing")

        name = yield db.get(self.context, '__name__')
        parent = yield db.get(self.context, '__parent__')

        cmd.write("%s %s\n" % (action_name, name))
        submitter = IVirtualizationContainerSubmitter(parent)

        try:
            yield submitter.submit(self.job, name)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))
            raise


class DiskspaceInvalidConfigError(KeyError):
    def __init__(self, msg):
        KeyError.__init__(self, msg)


class AllocateAction(ComputeAction):
    context(IUndeployed)
    action('allocate')

    _additional_keys = []

    @property
    def lock_keys(self):
        return [canonical_path(self.context),
                canonical_path(self.context.__parent__),
                canonical_path(self.context.__parent__.__parent__)] + self._additional_keys

    @defer.inlineCallbacks
    def _execute(self, cmd, args):

        if (yield db.ro_transact(IDeployed.providedBy)(self.context)):
            log.msg('Attempt to allocate a deployed compute: %s' % (self.context), system='deploy')
            return

        @db.ro_transact
        def get_matching_machines(container):
            all_machines = db.get_root()['oms_root']['machines']
            param = unicode(get_config().getstring('allocate', 'diskspace_filter_param',
                                                   default=u'/storage'))

            def condition_generator(m):
                yield ICompute.providedBy(m)
                yield find_compute_v12n_container(m, container)
                yield not getattr(m, 'exclude_from_allocation', None)
                yield self.context.memory_usage < m.memory
                yield sum(map(lambda (pk, pv): pv,
                              filter(lambda (pk, pv): pk != 'total',
                                     self.context.diskspace.iteritems()))) < m.diskspace.get(param, 0)
                yield self.context.num_cores <= m.num_cores
                yield self.context.template in map(lambda t: t.name,
                                                   filter(lambda t: ITemplate.providedBy(t),
                                                          m['templates'].listcontent()
                                                          if m['templates'] else []))

            def unwind_until_false(generator):
                fail_description = ['Not a compute',
                                    'No virt container %s' % container,
                                    'Excluded from allocation',
                                    'Has less than %s MB memory' % self.context.memory_usage,
                                    'Not enough diskspace',
                                    'Not enough CPU cores',
                                    'Template is unavailable']

                try:
                    for idx, r in enumerate(generator):
                        if not r:
                            return 'Fail at %d: %s' % (idx, fail_description[idx])
                    return 'Match'
                except Exception as e:
                    log.err(system='action-allocate')
                    return 'Fail (exception)' % (fail_description, e)

            results = map(lambda m: (str(m), unwind_until_false(condition_generator(m))), all_machines)
            log.msg('Searching in: %s' % (results), logLevel=DEBUG, system='action-allocate')

            return filter(lambda m: all(condition_generator(m)), all_machines)

        log.msg('Allocating %s: searching for targets...' % self.context, system='action-allocate')

        vmsbackend = yield db.ro_transact(lambda: self.context.__parent__.backend)()
        machines = yield get_matching_machines(vmsbackend)

        if len(machines) <= 0:
            self._action_log(cmd, 'Found no fitting machines. Action aborted.', system='action-allocate',
                             logLevel=WARNING)
            return

        @db.ro_transact
        def rank(machines):
            return sorted(machines, key=lambda m: m.__name__)

        best = (yield rank(machines))[0]
        log.msg('Found %s as the best candidate. Attempting to allocate...' % (best),
                system='action-allocate')

        bestvmscontainer = yield db.ro_transact(find_compute_v12n_container)(best, vmsbackend)

        @db.transact
        def set_additional_keys():
            self._additional_keys = [canonical_path(best), canonical_path(bestvmscontainer)]
        yield set_additional_keys()

        yield self.reacquire_until_clear()

        yield DeployAction(self.context)._execute(DetachedProtocol(), bestvmscontainer)


def mv_compute_model(context_path, target_path):
    try:
        vm = traverse1(context_path)
        destination = traverse1(target_path)

        if vm is None or vm.__parent__ is None or destination is None:
            log.msg('Source or destination not found: %s (%s) -> %s (%s)' %
                    (context_path, vm, target_path, destination), system='deploy')

        if vm.__parent__.__parent__ != destination.__parent__:
            destination.add(vm)
            log.msg('Model moved.', system='deploy')
        else:
            log.msg('Model NOT moved: VM already in destination', system='deploy')
    except IndexError:
        log.msg('Model NOT moved: destination compute or vms do not exist', system='deploy',
                logLevel=WARNING)
    except KeyError:
        log.msg('Model NOT moved: already moved by sync?', system='deploy')


class DeployAction(VComputeAction):
    context(IUndeployed)

    action('deploy')

    @db.ro_transact(proxy=False)
    def get_parameters(self):
        return {'template_name': self.context.template,
                'hostname': self.context.hostname,
                'vm_type': self.context.__parent__.backend,
                'uuid': self.context.__name__,
                'nameservers': db.remove_persistent_proxy(self.context.nameservers),
                'autostart': self.context.autostart,
                'ip_address': self.context.ipv4_address.split('/')[0],
                'passwd': getattr(self.context, 'root_password', None),
                'start_vm': getattr(self.context, 'start_vm', False),
                'memory': self.context.memory / 1024.0}

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        template = yield db.get(self.context, 'template')

        if not template:
            self._action_log(cmd, 'Cannot deploy %s (%s) because no template was specified' %
                             (self.context.hostname, self.context), system='deploy', logLevel=ERROR)
            return

        if (yield db.ro_transact(IDeployed.providedBy)(self.context)):
            log.msg('Attempt to deploy a deployed compute: %s' % (self.context), system='deploy')
            return

        @db.transact
        def allocate_ip_address():
            ippools = db.get_root()['oms_root']['ippools']
            ip = ippools.allocate()
            if ip is not None:
                self._action_log(cmd, 'Allocated IP: %s for %s' % (ip, self.context), system='deploy')
                ulog = UserLogger(principal=cmd.protocol.interaction.participations[0].principal,
                                  subject=self.context, owner=self.context.__owner__)
                ulog.log('Allocated IP for %s: %s' % (self.context, ip))
                return ip
            else:
                raise Exception('Could not allocate IP for the new compute: pools exhausted or undefined')

        @db.transact
        def cleanup_root_password():
            if getattr(self.context, 'root_password', None) is not None:
                self.context.root_password = None

        target = (args if IVirtualizationContainer.providedBy(args)
                  else (yield db.get(self.context, '__parent__')))

        @db.ro_transact(proxy=False)
        def get_current_ctid():
            ctidlist = db.get_root()['oms_root']['computes']['openvz']
            return max([follow_symlinks(symlink).ctid for symlink in ctidlist.listcontent()] + [100])

        try:
            yield db.transact(alsoProvides)(self.context, IDeploying)

            vm_parameters = yield self.get_parameters()

            if vm_parameters['ip_address'] in (None, u'0.0.0.0/32', u'0.0.0.0', '0.0.0.0/32', '0.0.0.0'):
                ipaddr = yield allocate_ip_address()
                vm_parameters.update({'ip_address': str(ipaddr)})
                # TODO: set IP to the model

            vms_backend = yield db.get(target, 'backend')
            if vms_backend == 'openvz':
                ctid = yield get_current_ctid()
                if ctid is not None:
                    log.msg('Deploying %s to %s: hinting CTID (%s)' % (self.context, target, ctid),
                            system='deploy')
                    vm_parameters.update({'ctid': ctid + 1})
                else:
                    self._action_log(cmd, 'Information about current global CTID is unavailable yet, '
                                     'will let it use HN-local value instead')

            log.msg('Deploying %s to %s: issuing agent command' % (self.context, target), system='deploy')
            res = yield IVirtualizationContainerSubmitter(target).submit(IDeployVM, vm_parameters)
            yield cleanup_root_password()

            # TODO: refactor (eliminate duplication with MigrateAction)
            name = yield db.get(self.context, '__name__')
            hostname = yield db.get(self.context, 'hostname')

            log.msg('Checking post-deploy...', system='deploy')

            if (yield self._check_vm_post(cmd, name, hostname, target)):

                @db.ro_transact
                def get_canonical_paths(context, target):
                    return (canonical_path(context), canonical_path(target))

                canonical_paths = yield get_canonical_paths(self.context, target)

                @db.transact
                def finalize_vm():
                    vm = traverse1(canonical_paths[0])
                    mv_compute_model(*canonical_paths)
                    noLongerProvides(vm, IUndeployed)
                    alsoProvides(vm, IDeployed)

                    self._action_log(cmd, 'Deployment of "%s" is finished' % (vm_parameters['hostname']),
                                     system='deploy')

                yield finalize_vm()
            else:
                self._action_log(cmd, 'Deployment result: %s' % res)

        finally:
            @db.transact
            def cleanup_deploying():
                noLongerProvides(self.context, IDeploying)
            yield cleanup_deploying()

    @defer.inlineCallbacks
    def _get_vmlist(self, destination_vms):
        dest_submitter = IVirtualizationContainerSubmitter(destination_vms)
        vmlist = yield dest_submitter.submit(IListVMS)
        defer.returnValue(vmlist)

    @defer.inlineCallbacks
    def _check_vm_post(self, cmd, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if not vmlist or (name not in map(lambda x: x['uuid'], vmlist)):
            self._action_log(cmd, 'Failed deployment of %s to %s: '
                             'VM not found in destination after deployment' % (name, destination_hostname))
            defer.returnValue(False)

        defer.returnValue(True)


class UndeployAction(VComputeAction):
    context(IDeployed)

    action('undeploy')

    @property
    def lock_keys(self):
        return (canonical_path(self.context),
                canonical_path(self.context.__parent__),
                canonical_path(self.context.__parent__.__parent__))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        name = yield db.get(self.context, '__name__')
        parent = yield db.get(self.context, '__parent__')

        submitter = IVirtualizationContainerSubmitter(parent)
        yield submitter.submit(IUndeployVM, name)

        @db.transact
        def finalize_vm():
            ippools = db.get_root()['oms_root']['ippools']
            ip = netaddr.IPAddress(self.context.ipv4_address.split('/')[0])
            if ippools.free(ip):
                ulog = UserLogger(principal=cmd.protocol.interaction.participations[0].principal,
                                  subject=self.context, owner=self.context.__owner__)
                ulog.log('Deallocated IP: %s', ip)

            vm = traverse1(canonical_path(self.context))
            if vm is not None:
                noLongerProvides(vm, IDeployed)
                alsoProvides(vm, IUndeployed)

        yield finalize_vm()


class MigrateAction(VComputeAction):
    context(IVirtualCompute)

    action('migrate')

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('dest_path')
        parser.add_argument('-o', '--offline', action='store_true', default=False,
                            help="Force offline migration, shutdown VM before migrating")
        return parser

    @defer.inlineCallbacks
    def _get_vmlist(self, destination_vms):
        dest_submitter = IVirtualizationContainerSubmitter(destination_vms)
        vmlist = yield dest_submitter.submit(IListVMS)
        defer.returnValue(vmlist)

    @defer.inlineCallbacks
    def _check_vm_pre(self, cmd, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if (name in map(lambda x: x['uuid'], vmlist)):
            self._action_log(cmd,
                             'Failed migration of %s to %s: destination already contains this VM'
                             % (name, destination_hostname))
            defer.returnValue(False)

        if ((yield db.get(self.context, 'ctid')) in map(lambda x: x.get('ctid'), vmlist)):
            self._action_log(cmd,
                             'Failed migration of %s to %s: destination container ID conflict'
                             % (name, destination_hostname))
            defer.renderValue(False)

        defer.returnValue(True)

    @defer.inlineCallbacks
    def _check_vm_post(self, cmd, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if (name not in map(lambda x: x['uuid'], vmlist)):
            self._action_log(cmd, 'Failed migration of %s to %s: VM not found in destination after migration'
                             % (name, destination_hostname))
            defer.returnValue(False)

        defer.returnValue(True)

    _additional_keys = []

    @property
    def lock_keys(self):
        # TODO: figure out how to best extend the list while the action is being executed with the
        # destination HN too
        return (canonical_path(self.context),
                canonical_path(self.context.__parent__),
                canonical_path(self.context.__parent__.__parent__)) + self._additional_keys

    @defer.inlineCallbacks
    def _execute(self, cmd, args):

        @db.ro_transact
        def get_destination():
            return (args.__parent__ if IVirtualizationContainer.providedBy(args)
                    else cmd.traverse(args.dest_path))

        @db.ro_transact
        def get_hostname(target):
            return target.hostname

        name = yield db.get(self.context, '__name__')
        source_vms = yield db.get(self.context, '__parent__')

        destination = yield get_destination()
        assert ICompute.providedBy(destination), 'Destination must be a Compute'
        assert not IVirtualCompute.providedBy(destination), 'Cannot migrate to a VM'
        destination_hostname = yield get_hostname(destination)
        destination_vms = follow_symlinks(destination['vms'])
        assert (yield db.get(destination_vms, 'backend')) == (yield db.get(source_vms, 'backend')),\
                'Destination backend is different from source'

        @db.transact
        def set_additional_keys():
            self._additional_keys = [canonical_path(destination_vms), canonical_path(destination)]
        yield set_additional_keys()

        yield self.reacquire_until_clear()

        log.msg('Initiating migration for %s to %s' % (name, destination_hostname), system='migrate')

        try:
            if not (yield self._check_vm_pre(name, destination_hostname, destination_vms)):
                return

            source_submitter = IVirtualizationContainerSubmitter(source_vms)
            yield source_submitter.submit(IMigrateVM, name, destination_hostname, (not args.offline), False)
            log.msg('Migration done. Checking... %s' % destination_vms, system='migrate')

            if (yield self._check_vm_post(cmd, name, destination_hostname, destination_vms)):
                log.msg('Migration finished successfully!', system='migrate')

                @db.transact
                def mv_and_inherit():
                    machines = db.get_root()['oms_root']['machines']
                    computes = db.get_root()['oms_root']['computes']
                    try:
                        destination_compute = machines[destination.__name__]
                        vm_compute = follow_symlinks(computes[self.context.__name__])
                        vm_compute.failure = destination_compute.failure
                        vm_compute.suspicious = destination_compute.suspicious
                        dvms = follow_symlinks(destination_compute['vms'])
                        dvms.add(vm_compute)
                        log.msg('Model moved.', system='migrate')
                    except IndexError:
                        log.msg('Model NOT moved: destination compute or vms do not exist', system='migrate')
                    except KeyError:
                        log.msg('Model NOT moved: already moved by sync?', system='migrate')
                yield mv_and_inherit()
        except OperationRemoteError as e:
            self._action_log(cmd, 'Failed migration of %s to %s: remote error %s' % (
                name, destination_hostname, '\n%s' % e.remote_tb if e.remote_tb else ''),
                logLevel=ERROR, system='migrate')


class InfoAction(VComputeAction):
    """This is a temporary command used to fetch realtime info"""
    context(IVirtualCompute)

    action('info')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context.__parent__,))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        name = yield db.get(self.context, '__name__')
        parent = yield db.get(self.context, '__parent__')

        submitter = IVirtualizationContainerSubmitter(parent)
        try:
            for vm in (yield submitter.submit(IListVMS)):
                if vm['uuid'] == name:
                    max_key_len = max(len(key) for key in vm)
                    for key, value in vm.items():
                        cmd.write("%s %s\n" % ((key + ':').ljust(max_key_len), value))
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


class StartComputeAction(VComputeAction):
    action('start')

    job = IStartVM


class ShutdownComputeAction(VComputeAction):
    action('shutdown')

    action_name = "shutting down"
    job = IShutdownVM


class DestroyComputeAction(VComputeAction):
    action('destroy')

    job = IDestroyVM


class SuspendComputeAction(VComputeAction):
    action('suspend')

    job = ISuspendVM


class ResumeAction(VComputeAction):
    action('resume')

    action_name = 'resuming'
    job = IResumeVM


class RebootAction(VComputeAction):
    action('reboot')

    job = IRebootVM


class StopAllVmsCmd(Cmd):
    implements(ICmdArgumentsSyntax)
    command('stopvms')

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('-u', help="Stop all VMs belonging to the user")
        return parser

    @db.ro_transact
    def get_computes(self, args):
        computes = db.get_root()['oms_root']['computes']
        user_vms = []
        for c in map(follow_symlinks, computes.listcontent()):
            if not IVirtualCompute.providedBy(c):
                continue
            if c.__owner__ == args.u:
                user_vms.append(c)
        return user_vms

    @require_admins_only
    @defer.inlineCallbacks
    def execute(self, args):
        log.msg('Stopping all VMs of "%s"...' % args.u, system='stopallvms')

        computes = yield self.get_computes(args)
        for c in computes:
            self.write("Stopping %s...\n" % c)
            yield ShutdownComputeAction(c).execute(DetachedProtocol(), object())

        self.write("Stopping done. %s VMs stopped\n" % (len(computes)))
        log.msg('Stopping done. %s VMs of "%s" stopped' % (len(computes), args.u), system='stopallvms')
