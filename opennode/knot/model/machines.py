from __future__ import absolute_import

from grokcore.component import context
from zope import schema
from zope.component import provideSubscriptionAdapter
from zope.interface import Interface, implements

from opennode.knot.model.compute import Compute
from opennode.knot.model.hangar import Hangar
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Container, ContainerInjector, ReadonlyContainer
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.security.directives import permissions


class Machines(Container):
    __contains__ = Compute
    __name__ = 'machines'

    def __init__(self):
        super(Machines, self).__init__()

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


class IncomingMachinesInjector(ContainerInjector):
    context(Machines)
    __class__ = IncomingMachines


class BaseIncomingMachines(ReadonlyContainer):
    """Template method abstract class for stack-specific incoming machines list implementations"""

    def _get(self):
        """ Provide list of incoming host names """
        raise NotImplemented

    @property
    def _items(self):
        items = self._get()
        pending = dict((h, IncomingMachineRequest(h)) for h in items)
        return pending


class HangarMachinesInjector(ContainerInjector):
    context(Machines)
    __class__ = Hangar


provideSubscriptionAdapter(ByNameContainerExtension, adapts=(Machines, ))
provideSubscriptionAdapter(ActionsContainerExtension, adapts=(IncomingMachineRequest, ))
