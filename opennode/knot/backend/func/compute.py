from __future__ import absolute_import

from certmaster import certmaster

from grokcore.component import context, subscribe
from twisted.internet import defer

from opennode.knot.backend.compute import format_error
from opennode.knot.model.compute import ICompute
from opennode.knot.model.machines import IIncomingMachineRequest, IncomingMachineRequest
from opennode.knot.model.compute import IFuncInstalled
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import IModelDeletedEvent
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.proc import registered_process
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db


class AcceptHostRequestAction(Action):
    """Accept request of the host for joining OMS/certmaster"""
    context(IIncomingMachineRequest)

    action('accept')

    def get_name(self, *args):
        return self._name

    def get_subject(self, *args, **kwargs):
        return self.context

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @registered_process(get_name, get_subject)
    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            cm = certmaster.CertMaster()
            yield cm.sign_this_csr("%s.csr" % self.context.hostname)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


class RejectHostRequestAction(Action):
    """Remove request of the host for joining OMS/certmaster"""
    context(IIncomingMachineRequest)

    action('reject')

    def get_name(self, *args):
        return self._name

    def get_subject(self, *args, **kwargs):
        return self.context

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @registered_process(get_name, get_subject)
    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            cm = certmaster.CertMaster()
            yield cm.remove_this_cert(self.context.hostname)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))



@subscribe(ICompute, IModelDeletedEvent)
def delete_compute(model, event):
    if IFuncInstalled.providedBy(model):
        blocking_yield(RejectHostRequestAction(
            IncomingMachineRequest(model.hostname)).execute(DetachedProtocol(), object()))
