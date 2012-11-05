from __future__ import absolute_import
import logging

from salt.cli.key import Key
from grokcore.component import context

from opennode.knot.model.machines import IncomingMachines, BaseIncomingMachines
from opennode.oms.model.model.base import  ContainerInjector


class SaltKeyAdaptor(Key):
    """ Adaptor for Salt key management logic """

    def __init__(self, opts):
        Key.__init__(self, opts)

    def getUnacceptedKeyNames(self):
        try:
            return self._keys('pre')
        except SystemExit:
            logging.error('Salt reported an error and terminated trying to retrieve unaccepted keys.')


class IncomingMachinesSalt(BaseIncomingMachines):
    __name__ = 'salt'

    def _get(self):
        return SaltKeyAdaptor().getUnacceptedKeyNames()


class IncomingMachinesSaltInjector(ContainerInjector):
    context(IncomingMachines)
    __class__ = IncomingMachinesSalt
