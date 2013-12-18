from grokcore.component import context
from twisted.internet import defer
from twisted.python import log
from zope.component import handle

import logging

from opennode.knot.backend.operation import IHostInterfaces
from opennode.knot.backend.operation import IListVMS
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.backend.compute import ComputeAction
from opennode.knot.model.compute import Compute, IVirtualCompute
from opennode.knot.model.compute import IUndeployed, IDeployed, IDeploying
from opennode.knot.model.compute import IManageable
from opennode.knot.model.network import NetworkInterface, BridgeInterface
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer

from opennode.oms.config import get_config
from opennode.oms.model.form import alsoProvides
from opennode.oms.model.form import noLongerProvides
from opennode.oms.model.model.actions import action
from opennode.oms.model.model.events import ModelDeletedEvent
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.model.model.symlink import Symlink
from opennode.oms.model.traversal import canonical_path
from opennode.oms.zodb import db


class SyncVmsAction(ComputeAction):
    """Force vms sync + sync host info"""
    context(IVirtualizationContainer)

    action('sync')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context.__parent__,))

    @property
    def lock_keys(self):
        """ Returns list of object-related 'hashes' to lock against. Overload in derived classes
        to add more objects """
        return (canonical_path(self.context),
                canonical_path(self.context.__parent__),)

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        @db.ro_transact
        def get_ifaces_job():
            host_compute = self.context.__parent__
            return IHostInterfaces(host_compute)

        ifaces = yield (yield get_ifaces_job()).run()

        yield self._sync_ifaces(ifaces)

        yield self._sync_vms(cmd)

    @defer.inlineCallbacks
    def _sync_vms(self, cmd):
        submitter = IVirtualizationContainerSubmitter(self.context)
        remote_vms = yield submitter.submit(IListVMS)
        yield self._sync_vms_transact(remote_vms)

    @db.transact
    def _sync_vms_transact(self, remote_vms):
        local_vms = [i for i in self.context.listcontent() if IVirtualCompute.providedBy(i)]

        remote_uuids = set(i['uuid'] for i in remote_vms)
        local_uuids = set(i.__name__ for i in local_vms)

        root = db.get_root()['oms_root']
        machines = root['machines']

        for vm_uuid in remote_uuids.difference(local_uuids):
            remote_vm = [rvm for rvm in remote_vms if rvm['uuid'] == vm_uuid][0]

            existing_machine = follow_symlinks(machines['by-name'][remote_vm['name']])
            if existing_machine:
                # XXX: this VM is a nested VM, for now let's hack it this way
                new_compute = Symlink(existing_machine.__name__, existing_machine)
                self.context._add(new_compute)
            else:
                log.msg('Adding virtual compute %s...' % vm_uuid,
                        system='v12n-sync', logLevel=logging.WARNING)
                new_compute = Compute(unicode(remote_vm['name']), unicode(remote_vm['state']))
                new_compute.__name__ = vm_uuid
                new_compute.template = unicode(remote_vm['template'])
                alsoProvides(new_compute, IVirtualCompute)
                alsoProvides(new_compute, IDeployed)

                # for now let's force new synced computes to not have salt installed
                # XXX: not sure if removing a parent interface will remove the child also
                noLongerProvides(new_compute, IManageable)
                self.context.add(new_compute)

        for vm_uuid in remote_uuids.intersection(local_uuids):
            noLongerProvides(self.context[vm_uuid], IUndeployed)
            alsoProvides(self.context[vm_uuid], IDeployed)

        for vm_uuid in local_uuids.difference(remote_uuids):
            if IDeploying.providedBy(self.context[vm_uuid]):
                log.msg("Don't delete undeployed VM while in IDeploying state", system='v12n')
                continue

            noLongerProvides(self.context[vm_uuid], IDeployed)
            alsoProvides(self.context[vm_uuid], IUndeployed)
            self.context[vm_uuid].state = u'inactive'

            if get_config().getboolean('sync', 'delete_on_sync'):
                log.msg("Deleting compute %s" % vm_uuid, system='v12n-sync', logLevel=logging.WARNING)
                compute = self.context[vm_uuid]
                del self.context[vm_uuid]
                handle(compute, ModelDeletedEvent(self.context))

        # TODO: eliminate cross-import between compute and v12ncontainer
        from opennode.knot.backend.compute import ICompute
        from opennode.knot.backend.syncaction import SyncAction

        # sync each vm
        for compute in self.context.listcontent():
            if not IVirtualCompute.providedBy(compute):
                continue

            log.msg('Attempting to sync %s' % compute, system='sync-vms')
            if not ICompute.providedBy(compute.__parent__.__parent__):
                log.msg('Inconsistent data: %s, Compute is expected. Attempting to fix %s'
                        % (compute.__parent__.__parent__, compute),
                        system='sync-vms', logLevel=logging.WARNING)

                compute.__parent__ = self.context

                log.msg('Fixing %s %s' % (compute,
                                          'successful!'
                                          if ICompute.providedBy(compute.__parent__.__parent__)
                                          else 'failed!'),
                        system='sync-vms', logLevel=logging.WARNING)

                if not ICompute.providedBy(compute.__parent__.__parent__):
                    return

            action = SyncAction(compute)

            matching = [rvm for rvm in remote_vms if rvm['uuid'] == compute.__name__]

            if not matching:
                continue

            remote_vm = matching[0]

            # todo delegate all this into the action itself
            default_console = action._default_console()
            action._sync_consoles()
            action.sync_owner_transact(remote_vm)
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
            iface_node.ipv4_address = interface['ip'] if 'ip' in interface else ''

            if interface['type'] == 'bridge':
                iface_node.members = interface['members']

            if interface.get('primary'):
                iface_node.primary = True

            host_compute.interfaces.add(iface_node)

        # modify interfaces
        for iface_name in remote_names.intersection(local_names):
            interface = ifaces_by_name[iface_name]
            iface_node = local_interfaces[iface_name]
            iface_node.ipv4_address = interface['ip'] if 'ip' in interface else ''
            iface_node.hw_address = interface['mac'] if 'mac' in interface else ''

            if interface.get('primary'):
                iface_node.primary = True

            # Currently doesn't handle when an interface changes type
            # it would be easier to have a code path that treats it as a removal + addition
            if interface['type'] == 'bridge' and isinstance(iface_node, BridgeInterface):
                iface_node.members = interface['members']

        # remove interfaces
        for iface_name in local_names.difference(remote_names):
            del local_interfaces[iface_name]
