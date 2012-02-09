from __future__ import absolute_import

from certmaster import certmaster

from zope.component import provideSubscriptionAdapter
from zope import schema
from zope.interface import Interface, implements
from grokcore.component import context

from opennode.oms.security.directives import permissions
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import  Container, ContainerInjector, ReadonlyContainer
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.knot.model.compute import Compute
from opennode.knot.model.hangar import Hangar


class Machines(Container):
    __contains__ = Compute
    __name__ = 'machines'

    def __init__(self):
        super(Machines, self).__init__()
        self.hangar = Hangar()
        self._add(self.hangar)

    def __str__(self):
        return 'Machines list'


class MachinesRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Machines


class IIncomingMachineRequest(Interface):
    hostname = schema.TextLine(title=u"Hostname", min_length=3)


class IncomingMachineRequest(ReadonlyContainer):
    implements(IIncomingMachineRequest)
    permissions(dict(hostname='read'))

    def __init__(self, hostname):
        self.__name__ = hostname
        self.hostname = hostname


class IncomingMachines(ReadonlyContainer):
    __name__ = 'incoming'

    @property
    def _items(self):
        cm = certmaster.CertMaster()
        pending = {}
        for h in cm.get_csrs_waiting():
            pending[h] = IncomingMachineRequest(h)
        return pending


class IncomingMachinesInjector(ContainerInjector):
    context(Machines)
    __class__ = IncomingMachines

provideSubscriptionAdapter(ByNameContainerExtension, adapts=(Machines, ))
provideSubscriptionAdapter(ActionsContainerExtension, adapts=(IncomingMachineRequest, ))
