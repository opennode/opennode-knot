from grokcore.component import context, baseclass
from grokcore.component import implements
from logging import DEBUG, WARNING, ERROR
from twisted.internet import defer
from twisted.python import log, failure
from uuid import uuid5, NAMESPACE_DNS

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

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context.__parent__.__parent__,))

    def locked(self):
        return str(self.context) in self._lock_registry

    def lock(self):
        d = defer.Deferred()
        self._lock_registry[str(self.context)] = (d, self)
        return d

    def _remove_lock(self, uuid):
        del self._lock_registry[uuid]

    def unlock(self, d):
        d.chainDeferred(self._lock_registry[str(self.context)][0])
        d.addBoth(lambda r: self._remove_lock(str(self.context)))
        return d

    @defer.inlineCallbacks
    def add_log_event(self, cmd, msg, *args, **kwargs):
        owner = yield db.get(self.context, '__owner__')
        ulog = UserLogger(principal=cmd.protocol.interaction.participations[0].principal,
                          subject=self.context, owner=owner)
        ulog.log(msg, *args, **kwargs)

    @defer.inlineCallbacks
    def handle_error(self, f, cmd):
        msg = '%s: "%s" executing "%s"' % (type(f.value).__name__, f.value, type(self).__name__)
        log.msg(msg, system='compute-action', logLevel=ERROR)
        log.err(f, system='compute-action')
        yield self.add_log_event(cmd, msg)
        defer.returnValue(f)

    @defer.inlineCallbacks
    def handle_action_done(self, r, cmd):
        msg = '%s finished' % type(self).__name__
        yield self.add_log_event(cmd, msg)
        log.msg(msg, system='compute-action')

    def execute(self, cmd, args):
        if self.locked():
            lock_action = self._lock_registry[str(self.context)][1]
            log.msg('%s: %s is locked. Scheduling to run after finish of a previous action: %s'
                    % (self, self.context, lock_action), system='compute-action')
            # XXX: must be self.execute(), not self._execute(), otherwise a deadlock may occur
            self._lock_registry[str(self.context)][0].addBoth(lambda r: self.execute(cmd, args))
            return self._lock_registry[str(self.context)][0]

        ld = self.lock()

        @defer.inlineCallbacks
        def cancel_action(e, cmd):
            e.trap(Exception)
            msg = 'Canceled executing "%s" due to validate_hook failure' % type(self).__name__
            cmd.write('%s\n' % msg)
            yield self.add_log_event(cmd, msg)

        try:
            d = self.validate_hook(cmd.protocol.interaction.participations[0].principal)
        except Exception:
            ld.errback(failure.Failure())
            ld.addErrback(cancel_action, cmd)
            self._remove_lock(str(self.context))
            return ld

        d.addCallback(lambda r: self._execute(cmd, args))
        d.addErrback(self.handle_error, cmd)
        d.addCallback(self.handle_action_done, cmd)
        return self.unlock(d)

    def _execute(self, cmd, args):
        """ Must be overloaded by child classes """
        raise NotImplementedError()


class VComputeAction(ComputeAction):
    """Common code for virtual compute actions."""
    context(IVirtualCompute)
    baseclass()

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


class DiskspaceInvalidConfigError(KeyError):
    def __init__(self, msg):
        KeyError.__init__(self, msg)


class AllocateAction(ComputeAction):
    context(IUndeployed)
    action('allocate')

    @defer.inlineCallbacks
    def _execute(self, cmd, args):

        @db.ro_transact
        def get_matching_machines(container):
            all_machines = db.get_root()['oms_root']['machines']
            param = unicode(get_config().getstring('allocate', 'diskspace_filter_param',
                                                   default=u'/storage'))

            def condition_generator(m):
                yield ICompute.providedBy(m)
                yield not m.exclude_from_allocation
                yield find_compute_v12n_container(m, container)
                yield self.context.memory_usage < m.memory
                yield sum(map(lambda (pk, pv): pv,
                              filter(lambda (pk, pv): pk != 'total',
                                     self.context.diskspace.iteritems()))) < m.diskspace.get(param, 0)
                yield self.context.num_cores <= m.num_cores
                yield self.context.template in map(lambda t: t.name,
                                                   filter(lambda t: ITemplate.providedBy(t),
                                                          m['templates'].listcontent()))

            def unwind_until_false(generator):
                for idx, r in enumerate(generator):
                    if not r:
                        return 'Fail at %d' % idx
                return 'Match'

            log.msg('Searching in (index of failed condition or MATCH): %s' % (
                map(lambda m: (m, unwind_until_false(condition_generator(m))), all_machines)),
                logLevel=DEBUG, system='action-allocate')

            return filter(lambda m: all(condition_generator(m)), all_machines)

        log.msg('Allocating %s: searching for allocation targets...' % self.context,
                system='action-allocate')

        vmsbackend = yield db.ro_transact(lambda: self.context.__parent__.backend)()
        machines = yield get_matching_machines(vmsbackend)

        if len(machines) <= 0:
            log.msg('Found no fitting machines to allocate to. Action aborted.', system='action-allocate')
            cmd.write('Found no fitting machines to allocate to. Aborting.\n')
            return

        @db.ro_transact
        def rank(machines):
            return sorted(machines, key=lambda m: m.__name__)

        best = (yield rank(machines))[0]

        log.msg('Found %s as the best candidate. Attempting to allocate...' % (best),
                system='action-allocate')

        bestvmscontainer = yield db.ro_transact(find_compute_v12n_container)(best, vmsbackend)

        yield DeployAction(self.context)._execute(DetachedProtocol(), bestvmscontainer)


@db.transact
def mv_compute_model(context_path, target_path):
    try:
        vm = traverse1(context_path)
        destination = traverse1(target_path)

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

    def log_error(self, cmd, msg):
        log.msg(msg, system='deploy')
        cmd.write(str(msg + '\n'))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        template = yield db.get(self.context, 'template')

        if not template:
            cmd.write("Cannot deploy %s because no template was specified\n" % self.context.hostname)
            log.msg('Cannot deploy %s (%s) because no template was specified' %
                    (self.context.hostname, self.context),
                    system='deploy-action', logLevel=ERROR)
            return

        @db.ro_transact
        def check_ip_address():
            log.msg('IP address on deployed VM: %s' % (self.context.ipv4_address), system='deploy')
            return self.context.ipv4_address not in (None, '0.0.0.0/32', '0.0.0.0')

        @db.ro_transact
        def allocate_ip_address():
            ippools = db.get_root()['oms_root']['ippools']
            ip = ippools.allocate()
            if ip is not None:
                log.msg('Allocated IP: %s for %s' % (ip, self.context), system='deploy')
                ulog = UserLogger(principal=cmd.protocol.interaction.participations[0].principal,
                                  subject=self.context, owner=self.context.__owner__)
                ulog.log('Allocated IP: %s', ip)
                return ip
            else:
                raise Exception('Could not allocate IP for the new compute: pools exhausted or undefined')

        @db.ro_transact(proxy=False)
        def get_parameters():
            return {'template_name': self.context.template,
                    'hostname': self.context.hostname,
                    'vm_type': self.context.__parent__.backend,
                    'uuid': self.context.__name__,
                    'nameservers': db.remove_persistent_proxy(self.context.nameservers),
                    'autostart': self.context.autostart,
                    'ip_address': self.context.ipv4_address.split('/')[0],
                    'passwd': getattr(self.context, 'root_password', None)}

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

            vm_parameters = yield get_parameters()

            if vm_parameters['ip_address'] in (None, u'0.0.0.0/32', u'0.0.0.0', '0.0.0.0/32', '0.0.0.0'):
                ipaddr = yield allocate_ip_address()
                vm_parameters.update({'ip_address': str(ipaddr)})

            vms_backend = yield db.get(target, 'backend')
            if vms_backend == 'openvz':
                log.msg('Deploying %s to %s: hinting CTID' % (self.context, target), system='deploy')
                ctid = yield get_current_ctid()
                if ctid is not None:
                    vm_parameters.update({'ctid': ctid + 1})
                else:
                    log.msg('Information about current global CTID is unavailable yet, will let it '
                            'use a local value instead', system='action-deploy', logLevel=WARNING)
                    cmd.write('Information about current global CTID is unavailable yet, '
                              'will let it use a local value instead\n')

            log.msg('Deploying %s to %s: issuing agent command' % (self.context, target), system='deploy')
            res = yield IVirtualizationContainerSubmitter(target).submit(IDeployVM, vm_parameters)
            yield cleanup_root_password()
            log.msg('IDeployVM result: %s' % res, system='deploy', logLevel=DEBUG)

            # TODO: refactor (eliminate duplication with MigrateAction)
            name = yield db.get(self.context, '__name__')
            hostname = yield db.get(self.context, 'hostname')

            log.msg('Checking post-deploy...', system='deploy')

            if (yield self._check_vm_post(cmd, name, hostname, target)):
                log.msg('Deployment finished successfully!', system='deploy')

                @db.ro_transact
                def get_canonical_paths(context, target):
                    return (canonical_path(context), canonical_path(target))

                yield mv_compute_model(*(yield get_canonical_paths(self.context, target)))

            @db.transact
            def finalize_vm():
                noLongerProvides(self.context, IUndeployed)
                alsoProvides(self.context, IDeployed)

                log.msg('Deployment of "%s" is finished' % (vm_parameters['hostname']), system='deploy')
                cmd.write("Changed state from undeployed to deployed\n")

            yield finalize_vm()
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
            self.log_error(cmd, 'Failed deployment of %s to %s: '
                           'VM not found in destination after deployment' % (name, destination_hostname))
            defer.returnValue(False)

        defer.returnValue(True)


class UndeployAction(VComputeAction):
    context(IDeployed)

    action('undeploy')

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        name = yield db.get(self.context, '__name__')
        parent = yield db.get(self.context, '__parent__')

        submitter = IVirtualizationContainerSubmitter(parent)
        res = yield submitter.submit(IUndeployVM, name)
        cmd.write('%s\n' % (res,))

        @db.transact
        def finalize_vm():
            ippools = db.get_root()['oms_root']['ippools']
            ip = netaddr.IPAddress(self.context.ipv4_address.split('/')[0])
            if ippools.free(ip):
                ulog = UserLogger(principal=cmd.protocol.interaction.participations[0].principal,
                                  subject=self.context, owner=self.context.__owner__)
                ulog.log('Deallocated IP: %s', ip)

            noLongerProvides(self.context, IDeployed)
            alsoProvides(self.context, IUndeployed)
            cmd.write('Changed state from deployed to undeployed\n')

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

    def log_error(self, cmd, msg):
        log.msg(msg, system='migrate')
        cmd.write(str(msg + '\n'))

    @defer.inlineCallbacks
    def _get_vmlist(self, destination_vms):
        dest_submitter = IVirtualizationContainerSubmitter(destination_vms)
        vmlist = yield dest_submitter.submit(IListVMS)
        defer.returnValue(vmlist)

    @defer.inlineCallbacks
    def _check_vm_pre(self, cmd, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if (name in map(lambda x: x['uuid'], vmlist)):
            self.log_error(cmd,
                           'Failed migration of %s to %s: destination already contains this VM'
                           % (name, destination_hostname))
            defer.returnValue(False)

        if ((yield db.get(self.context, 'ctid')) in map(lambda x: x.get('ctid'), vmlist)):
            self.log_error(cmd,
                           'Failed migration of %s to %s: destination container ID conflict'
                           % (name, destination_hostname))
            defer.renderValue(False)

        defer.returnValue(True)

    @defer.inlineCallbacks
    def _check_vm_post(self, cmd, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if (name not in map(lambda x: x['uuid'], vmlist)):
            self.log_error(cmd,
                           'Failed migration of %s to %s: VM not found in destination after migration'
                           % (name, destination_hostname))
            defer.returnValue(False)

        defer.returnValue(True)

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
            self.log_error(cmd, 'Failed migration of %s to %s: remote error %s' % (
                name, destination_hostname, '\n%s' % e.remote_tb if e.remote_tb else ''))


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
            # TODO: not efficient, improve
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

    @defer.inlineCallbacks
    def execute(self, args):
        log.msg('Stopping all VMs of "%s"...' % args.u)

        computes = yield self.get_computes(args)
        for c in computes:
            self.write("Stopping %s...\n" % c)
            yield ShutdownComputeAction(c).execute(DetachedProtocol(), object())

        self.write("Stopping done. %s VMs stopped\n" % (len(computes)))
