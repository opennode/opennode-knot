from __future__ import absolute_import



from grokcore.component import context

from opennode.knot.model import IncomingMachines, BaseIncomingMachines
from opennode.oms.model.model.base import  ContainerInjector


class IncomingMachinesSalt(BaseIncomingMachines):
    __name__ = 'salt'

    def _get(self):
        return {}

class IncomingMachinesSaltInjector(ContainerInjector):
    context(IncomingMachines)
    __class__ = IncomingMachinesSalt
