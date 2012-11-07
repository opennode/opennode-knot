from __future__ import absolute_import

from certmaster import certmaster
from grokcore.component import context
from twisted.internet import defer

from opennode.knot.backend.compute import register_machine
from opennode.knot.model.machines import BaseIncomingMachines, IncomingMachines
from opennode.knot.model.compute import IFuncInstalled
from opennode.oms.model.model.base import  ContainerInjector


class IncomingMachinesCertmaster(BaseIncomingMachines):
    __name__ = 'certmaster'

    def _get(self):
        cm = certmaster.CertMaster()
        return cm.get_csrs_waiting()

class IncomingMachinesInjector(ContainerInjector):
    context(IncomingMachines)
    __class__ = IncomingMachinesCertmaster


class RegisteredMachinesFunc(object):
    def _get(self):
        cm = certmaster.CertMaster()
        return cm.get_signed_certs()


@defer.inlineCallbacks
def import_machines():
    signed = RegisteredMachinesFunc()._get()
    for host in signed:
        yield register_machine(host, mgt_stack=IFuncInstalled)
