from __future__ import absolute_import

from zope.component import provideSubscriptionAdapter
from grokcore.component import context

from opennode.oms.model.model.base import  Container, ContainerInjector
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


provideSubscriptionAdapter(ByNameContainerExtension, adapts=(Machines, ))
