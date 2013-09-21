from grokcore.component import context, baseclass
from grokcore.component import implements, name
from grokcore.component import GlobalUtility
from logging import DEBUG, WARNING, ERROR
from twisted.internet import defer, error
from twisted.python import log
from uuid import uuid5, NAMESPACE_DNS
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility
from zope.component import getAllUtilitiesRegisteredFor

import netaddr
import time
import threading

from opennode.knot.backend import subprocess
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
from opennode.knot.model.common import IPreDeployHook
from opennode.knot.model.common import IPostUndeployHook
from opennode.knot.model.compute import ICompute, Compute, IVirtualCompute
from opennode.knot.model.compute import IUndeployed, IDeployed, IDeploying
from opennode.knot.model.compute import IManageable
from opennode.knot.model.template import ITemplate
from opennode.knot.model.user import IUserStatisticsProvider
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
from opennode.knot.utils import mac_addr_kvm_generator

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
from opennode.oms.model.model.stream import IStream
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
    _lock_registry_lock = threading.RLock()
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
        self._lock_registry_lock.acquire()

        try:
            d = defer.Deferred()
            log.msg('%s acquiring locks for: %s' % (self, self.lock_keys),
                    system='compute-action', logLevel=DEBUG)
            self._used_lock_keys = []
            for key in self.lock_keys:
                self._lock_registry[key] = (d, self)
                self._used_lock_keys.append(key)
            return d
        finally:
            self._lock_registry_lock.release()

    def reacquire(self):
        """ Add more locks for the currently executed action.

        Meant to be called from _execute to add locks for objects that are
        evaluated in _execute() and added to _lock_keys. Returns a deferred
        list of all lock deferreds, or None if no other action locks have been
        found, meaning that current action is safe to proceed."""
        self._lock_registry_lock.acquire()

        try:
            if not self.locked():
                return

            d = self._lock_registry[self.lock_keys[0]][0]
            deferred_list = []

            log.msg('%s adding locks: %s' % (self, self.lock_keys), system='compute-action', logLevel=DEBUG)

            for key in self.lock_keys:
                if key in self._lock_registry and self._lock_registry[key][0] is not d:
                    dother, actionother = self._lock_registry[key]
                    log.msg('Another action %s has locked %s... %s will wait until it finishes'
                        % (actionother, key, self), system='compute-action')
                    deferred_list.append(dother)

            log.msg('%s has locks: %s' % (self, self._used_lock_keys),
                    system='compute-action', logLevel=DEBUG)

            if len(deferred_list) > 0:
                return defer.DeferredList(deferred_list, consumeErrors=True)

            # Safer all-or-nothing approach to reacquire locking
            for key in self.lock_keys:
                if key not in self._lock_registry:
                    self._lock_registry[key] = (d, self)
                    self._used_lock_keys.append(key)
        finally:
            self._lock_registry_lock.release()

    @defer.inlineCallbacks
    def reacquire_until_clear(self):
        while True:
            dl = self.reacquire()

            if dl is None:
                break

            if self._do_not_enqueue:
                log.msg(' Skipping reacquire due to no-enqueue feature of %s' % (self))
                defer.returnValue(dl)

            log.msg('%s is waiting for other actions locking additional objects (%s)...'
                    % (self, self._additional_keys), system='compute-action')

            yield dl  # wait for any actions locking our targets

            log.msg('%s will attempt to reacquire locks again (%s)...'
                    % (self, self._additional_keys), system='compute-action')

    def _release_and_fire_next_now(self):
        if not hasattr(self, '_used_lock_keys'):
            return

        ld = None
        for key in self._used_lock_keys:
            if not ld:
                ld, _ = self._lock_registry[key]
            del self._lock_registry[key]

        del self._used_lock_keys

        ld.callback(None)
        return ld

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
        return self.add_log_event(cmd, '%s finished' % (self))

    def find_first(self):
        try:
            self._lock_registry_lock.acquire()
            for key in self.lock_keys:
                data = self._lock_registry.get(key)
                if data is not None:
                    return data
        finally:
            self._lock_registry_lock.release()

    def execute(self, cmd, args):
        try:
            self._lock_registry_lock.acquire()
            if self.locked():
                ld, lock_action = self.find_first()

                msg = '%s: one of %s is locked by %s.' % (self, self.lock_keys, lock_action)

                if self._do_not_enqueue:
                    self._action_log(cmd, msg + ' Skipping due to no-enqueue feature')
                    return ld

                self._action_log(cmd, msg + ' Scheduling to run after finish of previous action')

                def execute_on_unlock(r):
                    self._action_log(cmd, '%s: %s are unlocked. Executing now' % (self, self.lock_keys),
                                     logLevel=DEBUG)
                    # XXX: must be self.execute(), not self._execute(): next action must lock all its objects
                    self.execute(cmd, args)

                ld.addBoth(execute_on_unlock)
                return ld

            self.acquire()
        finally:
            self._lock_registry_lock.release()

        try:
            principal = cmd.protocol.interaction.participations[0].principal
            owner = getUtility(IAuthentication).getPrincipal(self.context.__owner__)
            d = defer.maybeDeferred(self.validate_hook, principal if principal.id != 'root' else owner)
        except Exception:
            log.err(system='compute-action-validate-hook')
            return self._release_and_fire_next_now()
        else:
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

    inprogress_marker = None
    state = None

    @property
    def lock_keys(self):
        return [canonical_path(self.context),
                canonical_path(self.context.__parent__),
                canonical_path(self.context.__parent__.__parent__)]

    @db.transact
    def set_inprogress(self):
        if self.inprogress_marker is not None:
            alsoProvides(self.context, self.inprogress_marker)
        if self.state is not None:
            self.context.state = self.state

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        action_name = getattr(self, 'action_name', self._name + "ing")

        name = yield db.get(self.context, '__name__')
        parent = yield db.get(self.context, '__parent__')

        yield self.set_inprogress()

        self._action_log(cmd, '%s %s' % (action_name, name))
        submitter = IVirtualizationContainerSubmitter(parent)

        try:
            yield submitter.submit(self.job, name)
        except Exception as e:
            self._action_log(cmd, '%s' % (format_error(e)))
            raise

    @db.ro_transact(proxy=False)
    def get_parameters(self):
        return {'template_name': self.context.template,
                'hostname': self.context.hostname,
                'vm_type': str(self.context.__parent__.backend),
                'uuid': str(self.context.__name__),
                'nameservers': db.remove_persistent_proxy(self.context.nameservers),
                'autostart': self.context.autostart,
                'ip_address': self.context.ipv4_address.split('/')[0],
                'passwd': str(getattr(self.context, 'root_password', None)),
                'start_vm': getattr(self.context, 'start_vm', False),
                'memory': self.context.memory / 1024.0 if self.context.memory is not None else 0.0,
                'swap': self.context.swap_size / 1024.0 if self.context.swap_size is not None else 0.0,
                'owner': self.context.__owner__,
                'disk': self.context.diskspace.get('root', 10000.0) / 1024.0,
                'vcpu': self.context.num_cores,
                'mac_address': getattr(self.context, 'mac_address', None)}


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
                if not get_config().getboolean('overcommit', 'memory', False):
                    yield self.context.memory_usage < m.memory
                else:
                    log.msg('Memory filtering is disabled.', system='action-allocate')
                if not get_config().getboolean('overcommit', 'disk', False):
                    yield sum(map(lambda (pk, pv): pv,
                              filter(lambda (pk, pv): pk != 'total',
                                     self.context.diskspace.iteritems()))) < (m.diskspace.get(param, 0) -
                                                                              m.diskspace_usage.get(param, 0))
                else:
                    log.msg('Diskspace filtering is disabled.', system='action-allocate')
                if not get_config().getboolean('overcommit', 'cores', False):
                    yield self.context.num_cores <= m.num_cores
                else:
                    log.msg('\'Total # of cores\' filtering is disabled.', system='action-allocate')

                templates = m['vms-%s' % container]['templates']
                yield self.context.template in map(lambda t: t.name,
                                                   filter(lambda t: ITemplate.providedBy(t),
                                                          templates.listcontent() if templates else []))

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


@db.ro_transact(proxy=False)
def get_current_ctid():
    ctidlist = db.get_root()['oms_root']['computes']['openvz']
    return max([follow_symlinks(symlink).ctid for symlink in ctidlist.listcontent()] + [100])


class DeployAction(VComputeAction):
    context(IUndeployed)

    action('deploy')

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

        try:
            yield db.transact(alsoProvides)(self.context, IDeploying)

            vm_parameters = yield self.get_parameters()

            ipaddr = netaddr.IPAddress(vm_parameters['ip_address'])
            if vm_parameters['ip_address'] in (None, u'0.0.0.0/32', u'0.0.0.0', '0.0.0.0/32', '0.0.0.0'):
                ipaddr = yield allocate_ip_address()
                vm_parameters.update({'ip_address': str(ipaddr)})

            utils = getAllUtilitiesRegisteredFor(IPreDeployHook)
            for util in utils:
                yield defer.maybeDeferred(util.execute, self.context, cmd, vm_parameters)

            log.msg('Deploying %s to %s: issuing agent command' % (self.context, target), system='deploy')
            res = yield IVirtualizationContainerSubmitter(target).submit(IDeployVM, vm_parameters)
            yield cleanup_root_password()

            name = yield db.get(self.context, '__name__')
            hostname = yield db.get(self.context, 'hostname')
            owner = yield db.get(self.context, '__owner__')
            owner_obj = getUtility(IAuthentication).getPrincipal(owner)

            log.msg('Checking post-deploy...', system='deploy')

            if not (yield self._check_vm_post(cmd, name, hostname, target)):
                self._action_log(cmd, 'Deployment failed. Deployment request result: %s' % res,
                                 system='deploy')
                return

            @db.transact
            def add_deployed_model_remove_from_hangar(c, target):
                path = canonical_path(target)
                target = traverse1(path)

                cpath = canonical_path(c)
                c = traverse1(cpath)
                if c is None:
                    raise Exception('Compute not found: "%s"' % cpath)

                new_compute = Compute(unicode(hostname), u'inactive')
                new_compute.__name__ = name
                new_compute.__owner__ = owner_obj
                new_compute.template = unicode(template)
                new_compute._ipv4_address = unicode(ipaddr)
                new_compute.mac_address = getattr(c, 'mac_address', None)

                alsoProvides(new_compute, IVirtualCompute)
                alsoProvides(new_compute, IDeployed)
                noLongerProvides(new_compute, IManageable)
                target.add(new_compute)

                container = c.__parent__
                del container[name]

                timestamp = int(time.time() * 1000)
                IStream(new_compute).add((timestamp, {'event': 'change',
                                                      'name': 'features',
                                                      'value': new_compute.features,
                                                      'old_value': self.context.features}))
                IStream(new_compute).add((timestamp, {'event': 'change',
                                                      'name': 'ipv4_address',
                                                      'value': new_compute._ipv4_address,
                                                      'old_value': self.context._ipv4_address}))

            yield add_deployed_model_remove_from_hangar(self.context, target)

            self._action_log(cmd, 'Deployment of "%s"(%s) is finished'
                             % (vm_parameters['hostname'], self.context.__name__), system='deploy')

            auto_allocate = get_config().getboolean('vms', 'auto_allocate', True)
            if not auto_allocate and not get_config().getboolean('stats', 'only_report_on_sync', True):
                yield defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner)

        except Exception as e:
            log.err(system='deploy')
            @db.transact
            def cleanup_deploying():
                noLongerProvides(self.context, IDeploying)
            yield cleanup_deploying()
            raise e

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


class PreDeployHookKVM(GlobalUtility):
    implements(IPreDeployHook)
    name('pre-deploy-kvm')

    @defer.inlineCallbacks
    def execute(self, context, *args, **kw):
        @db.ro_transact
        def check_backend(context):
            return context.__parent__.backend != 'kvm'

        if (yield check_backend(context)):
            return

        cmd = ['undefined']
        try:
            vm_parameters = args[1]

            secret = get_config().getstring('deploy', 'dhcp_key', 'secret')
            server = get_config().getstring('deploy', 'dhcp_server', 'localhost')
            server_port = get_config().getstring('deploy', 'dhcp_server_port', '7911')
            hook_script = get_config().getstring('deploy', 'hook_script_allocate',
                                                 'scripts/allocate_dhcp_ip.sh')

            @db.transact
            def ensure_compute_mac_address(context):
                mac_address = getattr(context, 'mac_address', None)
                if not mac_address:
                    mac_address = mac_addr_kvm_generator()
                    context.mac_address = unicode(mac_address)
                    vm_parameters.update({'mac_address': mac_address})
                return mac_address

            mac_address = yield ensure_compute_mac_address(context)

            cmd = [hook_script, secret, server, server_port, mac_address,
                   str(vm_parameters['ip_address']), vm_parameters['uuid']]

            yield subprocess.async_check_output(cmd)
        except error.ProcessTerminated:
            log.msg('Executing allocate_dhcp_ip.sh hook script failed: %s' % (cmd))
            log.err(system='deploy-hook-kvm')
            raise
        except Exception:
            log.err(system='deploy-hook-kvm')
            raise


class PreDeployHookOpenVZ(GlobalUtility):
    implements(IPreDeployHook)
    name('pre-deploy-openvz')

    @defer.inlineCallbacks
    def execute(self, context, *args, **kw):
        @db.ro_transact
        def check_backend(context):
            return context.__parent__.backend != 'openvz'

        if (yield check_backend(context)):
            return

        cmd = args[0]
        vm_parameters = args[1]

        ctid = yield get_current_ctid()

        if ctid is not None:
            log.msg('Deploying %s to %s: hinting CTID (%s)' % (context, context.__parent__, ctid),
                    system='deploy-hook-openvz')
            vm_parameters.update({'ctid': ctid + 1})
        else:
            self._action_log(cmd, 'Information about current global CTID is unavailable yet, '
                             'will let it use HN-local value instead', system='deploy-hook-openvz')


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

        vm_parameters = yield self.get_parameters()

        utils = getAllUtilitiesRegisteredFor(IPostUndeployHook)

        for util in utils:
            yield defer.maybeDeferred(util.execute, self.context, cmd, vm_parameters)


class PostUndeployHookKVM(GlobalUtility):
    implements(IPostUndeployHook)
    name('post-undeploy-kvm')

    @defer.inlineCallbacks
    def execute(self, context, *args, **kw):
        @db.ro_transact
        def check_backend(context):
            return context.__parent__.backend != 'kvm'

        if (yield check_backend(context)):
            return

        cmd = ['undefined']
        try:
            vm_parameters = args[1]

            secret = get_config().getstring('deploy', 'dhcp_key', 'secret')
            server = get_config().getstring('deploy', 'dhcp_server', 'localhost')
            server_port = get_config().getstring('deploy', 'dhcp_server_port', '7911')
            hook_script = get_config().getstring('deploy', 'hook_script_deallocate',
                                                 'scripts/deallocate_dhcp_ip.sh')

            mac_addr = getattr(context, 'mac_address')

            cmd = [hook_script, secret, server, server_port, mac_addr,
                   str(vm_parameters['ip_address']),
                   vm_parameters['uuid']]

            yield subprocess.async_check_output(cmd)
        except error.ProcessTerminated:
            log.msg('Executing allocate_dhcp_ip.sh hook script failed: %s' % (cmd))
            log.err(system='deploy-hook-kvm')
            raise
        except Exception:
            log.err(system='undeploy')
            raise


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
    state = u'starting'


class ShutdownComputeAction(VComputeAction):
    action('shutdown')

    action_name = "shutting down"
    job = IShutdownVM
    state = u'stopping'


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
