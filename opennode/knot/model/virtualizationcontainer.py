from __future__ import absolute_import

from grokcore.component import context
from grokcore.component import Subscription, baseclass
from zope import schema
from zope.component import provideSubscriptionAdapter
from zope.interface import Interface, implements

from opennode.knot.model.common import IInVirtualizationContainer
from opennode.knot.model.compute import ICompute, IVirtualCompute, IInCompute
from opennode.knot.model.computes import Computes
from opennode.knot.model.hangar import IInHangar
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Container
from opennode.oms.model.model.base import IContainerExtender
from opennode.oms.model.model.base import ReadonlyContainer
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.oms.model.model.search import ModelTags
from opennode.oms.model.model.symlink import Symlink
from opennode.oms.security.directives import permissions


class IVirtualizationContainer(Interface):
    backend = schema.Choice(title=u"Backend", values=(u'xen', u'kvm', u'openvz', u'lxc'))


class VirtualizationContainer(Container):
    implements(IVirtualizationContainer, IInCompute, IInHangar)
    permissions(dict(backend=('read', 'modify')))

    __contains__ = IVirtualCompute
    __markers__ = [IInVirtualizationContainer]

    def __init__(self, backend):
        super(VirtualizationContainer, self).__init__()
        self.backend = backend
        self.__name__ = 'vms-%s' % backend

    def __str__(self):
        return 'virtualizationcontainer-%s' % self.__name__


class VirtualizationContainerTags(ModelTags):
    context(VirtualizationContainer)

    def auto_tags(self):
        return [u'virt:' + self.context.backend]


class OpenVZContainer(ReadonlyContainer):
    permissions(dict(backend=('read', 'modify')))
    __name__ = 'openvz'
    __contains__ = IVirtualCompute

    @property
    def _items(self):
        # break an import cycle
        from opennode.oms.zodb import db
        machines = db.get_root()['oms_root']['machines']

        computes = {}

        def collect(container):
            from opennode.knot.model.machines import Machines

            seen = set()
            for item in container.listcontent():
                if ICompute.providedBy(item) and item.ctid is not None:
                    computes[str(item.ctid)] = Symlink(str(item.ctid), item)

                if (isinstance(item, Machines) or isinstance(item, Computes) or
                        ICompute.providedBy(item) or IVirtualizationContainer.providedBy(item)):
                    if item.__name__ not in seen:
                        seen.add(item.__name__)
                        collect(item)

        collect(machines)
        return computes


class OpenVZContainerExtension(Subscription):
    implements(IContainerExtender)
    baseclass()

    def extend(self):
        return {'openvz': OpenVZContainer(self.context)}


provideSubscriptionAdapter(OpenVZContainerExtension, adapts=(Computes, ))
provideSubscriptionAdapter(ActionsContainerExtension, adapts=(VirtualizationContainer, ))
provideSubscriptionAdapter(ByNameContainerExtension, adapts=(VirtualizationContainer, ))
