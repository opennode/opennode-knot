from grokcore.component import Adapter, context, implements
from twisted.internet import defer
from zope.component import handle
from zope.interface import Interface

from opennode.knot.backend.operation import IListVMS, IHostInterfaces, IStackInstalled
from opennode.knot.model.compute import IVirtualCompute, Compute, IDeployed, IUndeployed, IDeploying
from opennode.knot.model.network import NetworkInterface, BridgeInterface
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
from opennode.oms.config import get_config
from opennode.oms.model.form import ModelDeletedEvent, alsoProvides, noLongerProvides
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.symlink import Symlink, follow_symlinks
from opennode.oms.zodb import db

backends = {'test': 'test:///tmp/salt_vm_test_state.xml',
            'openvz': 'openvz:///system',
            'kvm': 'qemu:///system',
            'xen': 'xen:///'}


class IVirtualizationContainerSubmitter(Interface):
    def submit(job_interface):
        """Submits a job to the virtualization container"""


class VirtualizationContainerSubmitter(Adapter):
    implements(IVirtualizationContainerSubmitter)
    context(IVirtualizationContainer)

    @defer.inlineCallbacks
    def submit(self, job_interface, *args):

        # we cannot return a deferred from a db.transact
        @db.ro_transact
        def get_job():
            job = job_interface(self.context.__parent__)
            backend_uri = backends.get(self.context.backend, self.context.backend)
            return (job, backend_uri)

        job, backend_uri = yield get_job()
        res = yield job.run(backend_uri, *args)
        defer.returnValue(res)


class ListVirtualizationContainerAction(Action):
    """Lists the content of a virtualizationcontaineraction.
    Usually the zodb will be in sync, but it can be useful to see real time info (perhaps just for test)."""

    context(IVirtualizationContainer)
    action('list')

    @defer.inlineCallbacks
    def execute(self, cmd, args):
        cmd.write("listing virtual machines\n")
        db.assert_proxy(self.context)

        submitter = IVirtualizationContainerSubmitter(self.context)

        try:
            vms = yield submitter.submit(IListVMS)
        except Exception as e:
            cmd.write("%s\n" %
                      (": ".join(str(msg) for msg in e.args
                                 if (not isinstance(msg, str) or not msg.startswith('  File "/')))))
            return

        max_key_len = max([0] + [len(vm['name']) for vm in vms])

        for vm in vms:
            vm['name'] = vm['name'].ljust(max_key_len)
            cmd.write("%(name)s:  state=%(state)s, run_state=%(run_state)s, uuid=%(uuid)s, "
                      "memory=%(memory)s, template=%(template)s\n" % vm)

            if vm['diskspace']:
                cmd.write(" %s    storage:\n" % (' ' * max_key_len))
                for storage in vm['diskspace']:
                    cmd.write(" %s      %s = %s\n" % (' ' * max_key_len, storage, vm['diskspace'][storage]))

            if vm['consoles']:
                cmd.write(" %s    consoles:\n" % (' ' * max_key_len))
            for console in vm['consoles']:
                attrs = " ".join(["%s=%s" % pair for pair in console.items()])
                cmd.write(" %s      %s\n" % (' ' * max_key_len, attrs))


class SyncVmsAction(Action):
    """Force vms sync + sync host info"""
    context(IVirtualizationContainer)

    action('sync')

    @defer.inlineCallbacks
    def execute(self, cmd, args):
        # sync host interfaces (this is not the right place, but ...)
        @db.ro_transact
        def get_ifaces_job():
            host_compute = self.context.__parent__
            return IHostInterfaces(host_compute)

        ifaces = yield (yield get_ifaces_job()).run()

        yield self._sync_ifaces(ifaces)

        # sync virtual computes
        yield self._sync_vms(cmd)

    @defer.inlineCallbacks
    def _sync_vms(self, cmd):
        submitter = IVirtualizationContainerSubmitter(self.context)

        remote_vms = yield submitter.submit(IListVMS)
        yield self._sync_vms_2(remote_vms)

    @db.transact
    def _sync_vms_2(self, remote_vms):
        local_vms = [i for i in self.context.listcontent() if IVirtualCompute.providedBy(i)]

        remote_uuids = set(i['uuid'] for i in remote_vms)
        local_uuids = set(i.__name__ for i in local_vms)

        if not self.context._p_jar:
            return

        machines = self.context._p_jar.root()['oms_root']['machines']

        for vm_uuid in remote_uuids.difference(local_uuids):
            remote_vm = [i for i in remote_vms if i['uuid'] == vm_uuid][0]

            existing_machine = follow_symlinks(machines['by-name'][remote_vm['name']])
            if existing_machine:
                # XXX: this VM is a nested VM, for now let's hack it this way
                new_compute = Symlink(existing_machine.__name__, existing_machine)
                self.context._add(new_compute)
            else:
                new_compute = Compute(unicode(remote_vm['name']), unicode(remote_vm['state']))
                new_compute.__name__ = vm_uuid
                new_compute.template = unicode(remote_vm['template'])
                alsoProvides(new_compute, IVirtualCompute)
                alsoProvides(new_compute, IDeployed)

                # for now let's force synced computes to not have salt/func installed
                # XXX: not sure if removing a parent interface will remove the child also
                noLongerProvides(new_compute, IStackInstalled)
                self.context.add(new_compute)

        for vm_uuid in remote_uuids.intersection(local_uuids):
            noLongerProvides(self.context[vm_uuid], IUndeployed)
            alsoProvides(self.context[vm_uuid], IDeployed)

        for vm_uuid in local_uuids.difference(remote_uuids):
            if IDeploying.providedBy(self.context[vm_uuid]):
                print "[sync] don't delete undeployed VM while in IDeploying state"
                continue

            noLongerProvides(self.context[vm_uuid], IDeployed)
            alsoProvides(self.context[vm_uuid], IUndeployed)
            self.context[vm_uuid].state = u'inactive'

            if get_config().getboolean('sync', 'delete_on_sync'):
                print "[sync] Deleting compute", vm_uuid
                compute = self.context[vm_uuid]
                del self.context[vm_uuid]
                handle(compute, ModelDeletedEvent(self.context))

        # sync each vm
        from opennode.knot.backend.func.compute import SyncAction
        for action in [SyncAction(i) for i in self.context.listcontent() if IVirtualCompute.providedBy(i)]:
            matching = [i for i in remote_vms if i['uuid'] == action.context.__name__]
            if not matching:
                continue
            remote_vm = matching[0]

            # todo delegate all this into the action itself
            default_console = action._default_console()
            action._sync_consoles()
            action.sync_vm(remote_vm)
            action.create_default_console(default_console)

    @db.transact
    def _sync_ifaces(self, ifaces):
        host_compute = self.context.__parent__

        local_interfaces = host_compute.interfaces
        local_names = set(i.__name__ for i in local_interfaces)
        remote_names = set(i['name'] for i in ifaces)

        ifaces_by_name = dict((i['name'], i) for i in ifaces)

        # add interfaces
        for iface_name in remote_names.difference(local_names):
            interface = ifaces_by_name[iface_name]

            cls = NetworkInterface
            if interface['type'] == 'bridge':
                cls = BridgeInterface

            iface_node = cls(interface['name'], None, interface.get('mac', None), 'active')

            if 'ip' in interface:
                iface_node.ipv4_address = interface['ip']
            if interface['type'] == 'bridge':
                iface_node.members = interface['members']

            if interface.get('primary'):
                iface_node.primary = True

            host_compute.interfaces.add(iface_node)

        # modify interfaces
        for iface_name in remote_names.intersection(local_names):
            interface = ifaces_by_name[iface_name]
            iface_node = local_interfaces[iface_name]

            if 'ip' in interface:
                iface_node.ipv4_address = interface['ip']
            else:
                iface_node.ipv4_address = ''

            if 'mac' in interface:
                iface_node.hw_address = interface['mac']
            else:
                iface_node.hw_address = ''

            if interface.get('primary'):
                iface_node.primary = True

            # Currently doesn't handle when an interface changes type
            # it would be easier to have a code path that treats it as a removal + addition
            if interface['type'] == 'bridge' and isinstance(iface_node, BridgeInterface):
                iface_node.members = interface['members']


        # remove interfaces
        for iface_name in local_names.difference(remote_names):
            del local_interfaces[iface_name]
