from __future__ import absolute_import
import subprocess

from grokcore.component import context, subscribe
from twisted.internet import defer
from twisted.python import log

from opennode.knot.backend.compute import format_error
from opennode.knot.model.machines import IIncomingMachineRequest
from opennode.knot.model.machines import IncomingMachineRequest
from opennode.knot.model.compute import ICompute
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import IModelDeletedEvent
from opennode.oms.model.model.actions import Action, action
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db


class BaseHostRequestAction(Action):
    """Base host request action class"""
    context(IIncomingMachineRequest)

    _action = None
    _remote_option = None

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context, ))

    @defer.inlineCallbacks
    def execute(self, cmd, args):
        hostname = yield db.get(self.context, 'hostname')
        remote_salt_key_cmd = get_config().get('salt', 'remote_key_command', None)
        if remote_salt_key_cmd:
            try:
                output = subprocess.check_output([remote_salt_key_cmd, self._remote_option, hostname,
                                                  '--no-color', '--out=raw'])
                log.msg('Salt output: %s' % output, system='action-accept')
            except subprocess.CalledProcessError as e:
                cmd.write("%s\n" % format_error(e))
        else:
            try:
                import salt.config
                from salt.key import Key
                c_path = get_config().get('salt', 'master_config_path', '/etc/salt/master')
                opts = salt.config.client_config(c_path)
                yield getattr(Key(opts), self._action)(hostname)
            except Exception as e:
                cmd.write("%s\n" % format_error(e))


class AcceptHostRequestAction(BaseHostRequestAction):
    """Accept request of the host for joining OMS/Salt"""
    action('accept')
    _action = 'accept'
    _remote_option = '-a'


class RejectHostRequestAction(BaseHostRequestAction):
    """Remove request of the host for joining OMS/Salt"""
    action('reject')
    _action = 'reject'
    _remote_option = '-r'


@subscribe(ICompute, IModelDeletedEvent)
def delete_compute(model, event):
    if ISaltInstalled.providedBy(model):
        blocking_yield(RejectHostRequestAction(
            IncomingMachineRequest(model.hostname)).execute(DetachedProtocol(), object()))
