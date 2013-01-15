from __future__ import absolute_import

from grokcore.component import context, subscribe
from twisted.internet import defer

import salt.config
from salt.key import Key

from opennode.knot.backend.compute import format_error
from opennode.knot.model.compute import ICompute
from opennode.knot.model.machines import IIncomingMachineRequest, IncomingMachineRequest
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import IModelDeletedEvent
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.proc import registered_process
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db


class AcceptHostRequestAction(Action):
    """Accept request of the host for joining OMS/Salt"""
    context(IIncomingMachineRequest)

    action('accept')

    @db.ro_transact
    def get_name(self, *args):
        return self._name

    @db.ro_transact
    def get_subject(self, *args, **kwargs):
        return tuple((self.context, ))

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @registered_process(get_name, get_subject)
    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            c_path = get_config().get('salt', 'master_config_path', '/etc/salt/master')
            opts = salt.config.client_config(c_path)
            key = Key(opts)
            yield key.accept(self.context.hostname)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


class RejectHostRequestAction(Action):
    """Remove request of the host for joining OMS/Salt"""
    context(IIncomingMachineRequest)

    action('reject')

    @db.ro_transact
    def get_name(self, *args):
        return self._name

    @db.ro_transact
    def get_subject(self, *args, **kwargs):
        return tuple((self.context, ))

    @db.transact
    def execute(self, cmd, args):
        blocking_yield(self._execute(cmd, args))

    @registered_process(get_name, get_subject)
    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        try:
            c_path = get_config().get('salt', 'master_config_path', '/etc/salt/master')
            opts = salt.config.client_config(c_path)
            key = Key(opts)
            yield key.reject(self.context.hostname)
        except Exception as e:
            cmd.write("%s\n" % format_error(e))


@subscribe(ICompute, IModelDeletedEvent)
def delete_compute(model, event):
    if ISaltInstalled.providedBy(model):
        blocking_yield(RejectHostRequestAction(
            IncomingMachineRequest(model.hostname)).execute(DetachedProtocol(), object()))

