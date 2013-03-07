from grokcore.component import context, subscribe, baseclass
from logging import DEBUG, WARNING
from twisted.internet import defer
from twisted.python import log
from uuid import uuid5, NAMESPACE_DNS
from zope.component import handle

from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
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
from opennode.knot.backend.operation import IUpdateVM
from opennode.knot.backend.operation import OperationRemoteError
from opennode.knot.model.compute import ICompute, Compute, IVirtualCompute
from opennode.knot.model.compute import IUndeployed, IDeployed, IDeploying
from opennode.knot.model.compute import IManageable
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer

from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.model.form import IModelModifiedEvent
from opennode.oms.model.form import IModelDeletedEvent
from opennode.oms.model.form import IModelCreatedEvent
from opennode.oms.model.form import ModelModifiedEvent
from opennode.oms.model.form import alsoProvides
from opennode.oms.model.form import noLongerProvides
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.util import blocking_yield, exception_logger
from opennode.oms.zodb import db


def any_stack_installed(context):
    return IManageable.providedBy(context)


def format_error(e):
    return (": ".join(msg for msg in e.args if isinstance(msg, str) and not msg.startswith('  File "/')))


@defer.inlineCallbacks
def register_machine(host, mgt_stack=None):

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

    if not (yield check()):
        yield update()


def find_compute_v12n_container(compute, backend):
    for v12nc in compute:
        if IVirtualizationContainer.providedBy(v12nc) and v12nc.backend == backend:
            return v12nc


class ComputeAction(Action):
    context(ICompute)
    baseclass()

    _lock_registry = {}

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context.__parent__.__parent__,))

    def locked(self):
        return self.context in self._lock_registry

    def lock(self):
        self._lock_registry[self.context] = defer.Deferred()

    def unlock(self, d):
        d.chainDeferred(self._lock_registry[self.context])

    def execute(self, cmd, args):
        if self.locked():
            log.msg('%s is locked. Scheduling to run after finish of a previous action' % self.context,
                    system='compute-action')
            self._lock_registry[self.context].addBoth(lambda r: self.execute(cmd, args))
            return self._lock_registry[self.context]
        self.lock()
        d = self._execute(cmd, args)
        self.unlock(d)
        return d

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

            if param not in self.context.diskspace:
                raise KeyError(param)

            log.msg('Searching in: %s' % (
                map(lambda m: (m, (self.context.memory_usage < getattr(m, 'memory', None),
                                   self.context.diskspace[param] < getattr(m, 'diskspace', {}).get(param, 0),
                                   self.context.num_cores <= getattr(m, 'num_cores', None))), all_machines)),
                logLevel=DEBUG, system='action-allocate')

            return filter(lambda m: (ICompute.providedBy(m) and
                                     find_compute_v12n_container(m, container) and
                                     self.context.memory_usage < m.memory and
                                     self.context.diskspace[param] < m.diskspace.get(param, 0) and
                                     self.context.num_cores <= m.num_cores), all_machines)

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

        yield DeployAction(self.context).execute(DetachedProtocol(), bestvmscontainer)


class DeployAction(VComputeAction):
    context(IUndeployed)

    action('deploy')

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        template = yield db.get(self.context, 'template')

        if not template:
            cmd.write("Cannot deploy %s because no template was specified\n" % self.context.hostname)
            defer.returnValue(None)

        @db.ro_transact(proxy=False)
        def get_parameters():
            return {'template_name': self.context.template,
                    'hostname': self.context.hostname,
                    'vm_type': self.context.__parent__.backend,
                    'uuid': self.context.__name__,
                    'nameservers': db.remove_persistent_proxy(self.context.nameservers),
                    'autostart': self.context.autostart,
                    'ip_address': self.context.ipv4_address.split('/')[0],
                    'passwd': getattr(self.context, 'root_password', None),}

        @db.transact
        def cleanup_root_password():
            if getattr(self.context, 'root_password', None) is not None:
                self.context.root_password = None

        target = (args if IVirtualizationContainer.providedBy(args)
                  else (yield db.get(self.context, '__parent__')))

        @db.ro_transact(proxy=False)
        def get_current_ctid():
            proc = db.get_root()['proc']
            if 'openvz_ctid' not in proc:
                return None
            return db.get_root()['proc']['openvz_ctid'].ident

        try:
            yield db.transact(alsoProvides)(self.context, IDeploying)
            vm_parameters = yield get_parameters()
            vms_backend = yield db.get(target, 'backend')

            if vms_backend == 'openvz':
                ctid = yield get_current_ctid()
                if ctid is not None:
                    vm_parameters.update({'ctid': ctid + 1})
                else:
                    log.msg('Information about current global CTID is unavailable yet, will not hint agent '
                            'and let it use a local value instead', system='action-deploy',
                            logLevel=WARNING)
                    cmd.write('Information about current global CTID is unavailable yet, will not hint agent '
                              'and let it use a local value instead\n')

            res = yield IVirtualizationContainerSubmitter(target).submit(IDeployVM, vm_parameters)
            yield cleanup_root_password()
            log.msg('IDeployVM result: %s' % res, system='action-deploy')

            @db.transact
            def finalize_vm():
                if vms_backend == 'openvz':
                    db.get_root()['proc']['openvz_ctid'].ident += 1
                    log.msg('Set OpenVZ CTID to %s for %s' % (db.get_root()['proc']['openvz_ctid'].ident,
                                                              self.context.hostname), system='deploy')

                noLongerProvides(self.context, IDeploying)
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
            noLongerProvides(self.context, IDeployed)
            alsoProvides(self.context, IUndeployed)
            cmd.write("changed state from deployed to undeployed\n")

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

    def handle_error(self, cmd, msg):
        log.msg(msg, system='migrate')
        cmd.write(str(msg + '\n'))

    @defer.inlineCallbacks
    def _get_vmlist(self, destination_vms):
        dest_submitter = IVirtualizationContainerSubmitter(destination_vms)
        vmlist = yield dest_submitter.submit(IListVMS)
        defer.returnValue(vmlist)


    @defer.inlineCallbacks
    def _check_vm_prior(self, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if (name in map(lambda x: x['uuid'], vmlist)):
            self.handle_error(self.cmd,
                              'Failed migration of %s to %s: destination already contains this VM'
                              % (name, destination_hostname))
            defer.returnValue(False)

        if ((yield db.get(self.context, 'ctid')) in map(lambda x: x.get('ctid'), vmlist)):
            self.handle_error(self.cmd,
                              'Failed migration of %s to %s: destination container ID conflict'
                              % (name, destination_hostname))
            defer.renderValue(False)

        defer.returnValue(True)

    def _check_vm_post(self, name, destination_hostname, destination_vms):
        vmlist = yield self._get_vmlist(destination_vms)

        if (name not in map(lambda x: x['uuid'], vmlist)):
            self.handle_error(self.cmd,
                              'Failed migration of %s to %s: VM not found in destination after migration'
                              % (name, destination_hostname))
            defer.returnValue(False)

        defer.returnValue(True)


    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        self.cmd = cmd

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
                defer.returnValue(None)

            source_submitter = IVirtualizationContainerSubmitter(source_vms)
            yield source_submitter.submit(IMigrateVM, name, destination_hostname, (not args.offline), False)
            log.msg('Migration done. Checking... %s' % destination_vms, system='migrate')

            if (yield self._check_vm_post(destination_vms)):
                log.msg('Migration finished successfully!', system='migrate')

                @db.transact
                def mv():
                    machines = db.get_root()['oms_root']['machines']
                    computes = db.get_root()['oms_root']['computes']
                    try:
                        destination_compute = machines[destination.__name__]
                        vm_compute = follow_symlinks(computes[self.context.__name__])
                        dvms = follow_symlinks(destination_compute['vms'])
                        dvms.add(vm_compute)
                        log.msg('Model moved.', system='migrate')
                    except IndexError:
                        log.msg('Model NOT moved: destination compute or vms do not exist', system='migrate')
                    except KeyError:
                        log.msg('Model NOT moved: already moved by sync?', system='migrate')
                yield mv()
        except OperationRemoteError as e:
            self.handle_error(cmd, 'Failed migration of %s to %s: remote error %s' % (
                name, destination_hostname, '\n%s' % e.remote_tb if e.remote_tb else ''))
            defer.returnValue(None)


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


@subscribe(ICompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_compute_state_change_request(compute, event):

    if not event.modified.get('state', None):
        defer.returnValue(None)

    def get_action(original, modified):
        action_mapping = {'inactive': {'active': IStartVM},
                          'suspended': {'active': IResumeVM},
                          'active': {'inactive': IShutdownVM,
                                     'suspended': ISuspendVM}}

        action = action_mapping.get(original, {}).get(modified, None)
        return action

    original = event.original['state']
    modified = event.modified['state']
    action = get_action(original, modified)

    if not action:
        defer.returnValue(None)

    submitter = IVirtualizationContainerSubmitter(compute.__parent__)
    try:
        yield submitter.submit(action, compute.__name__)
    except Exception:
        compute.effective_state = event.original['state']
        raise
    else:
        compute.effective_state = event.modified['state']

    handle(compute, ModelModifiedEvent({'effective_state': event.original['state']},
                                       {'effective_state': compute.effective_state}))


@subscribe(IVirtualCompute, IModelDeletedEvent)
def delete_virtual_compute(model, event):
    if not ICompute.providedBy(model.__parent__.__parent__):
        return

    if IDeployed.providedBy(model):
        log.msg('deleting compute %s which is in IDeployed state, shutting down and '
                'undeploying first' % model.hostname, system='compute_backend')
        blocking_yield(DestroyComputeAction(model).execute(DetachedProtocol(), object()), timeout=20000)
        blocking_yield(UndeployAction(model).execute(DetachedProtocol(), object()), timeout=20000)
    else:
        log.msg('deleting compute %s which is already in IUndeployed state' %
                model.hostname, system='compute_backend')


@subscribe(IVirtualCompute, IModelCreatedEvent)
def create_virtual_compute(model, event):
    # TODO: maybe raise an exception here instead?
    if not IVirtualizationContainer.providedBy(model.__parent__):
        return

    if not ICompute.providedBy(model.__parent__.__parent__):
        return

    if IDeployed.providedBy(model):
        return

    log.msg('Deploying VM "%s"' % model, system='deploy')
    exception_logger(DeployAction(model).execute)(DetachedProtocol(), object())


@subscribe(IVirtualCompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_virtual_compute_config_change_request(compute, event):
    update_param_whitelist = ['cpu_limit',
                              'memory',
                              'num_cores',
                              'swap_size']

    params_to_update = filter(lambda (k, v): k in update_param_whitelist, event.modified.iteritems())

    if len(params_to_update) == 0:
        return

    update_values = [v for k, v in sorted(params_to_update, key=lambda (k, v): k)]

    submitter = IVirtualizationContainerSubmitter((yield db.get(compute, '__parent__')))
    try:
        yield submitter.submit(IUpdateVM, (yield db.get(compute, '__name__')), *update_values)
    except Exception:
        for mk, mv in event.modified.iteritems():
            setattr(compute, mk, event.original[mk])
        raise
