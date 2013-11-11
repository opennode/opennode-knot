from __future__ import absolute_import
import logging
import subprocess

from grokcore.component import context, implements, name, GlobalUtility

try:
    from salt.config import master_config
    from salt.key import Key
    from salt.utils.parsers import ConfigDirMixIn
except ImportError:
    # Allow missing salt on local machine
    master_config = lambda x: None

    class ConfigDirMixIn(object):
        """ Missing Salt """

    class Key(object):
        """ Missing Salt """

from twisted.internet import defer

from opennode.knot.backend.compute import register_machine
from opennode.knot.model.backend import IKeyManager
from opennode.knot.model.machines import IncomingMachines, BaseIncomingMachines
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.config import get_config
from opennode.oms.model.model.base import ContainerInjector


class DummyOptions(object):
    """ Dummy OptionParser placeholder """

    def _dummycall(self, *args, **kwargs):
        pass

    def __getattr__(self, name):
        if name == "__call__":
            return self._dummycall
        if name == 'config_dir':
            return '/etc/salt'
        return None


class BaseSaltKeyAdapter(object):
    REJECTED = 'minions_rejected'
    ACCEPTED = 'minions'
    UNACCEPTED = 'minions_pre'

    def _getKeyNames(self, ktype):
        raise NotImplementedError()

    def getUnacceptedKeyNames(self):
        return self._getKeyNames(self.UNACCEPTED)

    def getAcceptedKeyNames(self):
        return self._getKeyNames(self.ACCEPTED)

    def getRejectedKeyNames(self):
        return self._getKeyNames(self.REJECTED)


class SaltKeyAdapter(Key, ConfigDirMixIn, BaseSaltKeyAdapter):
    """ Adaptor for Salt key management logic """

    def __init__(self):
        self.options = DummyOptions()
        keys_config = master_config(self.get_config_file_path('master'))
        Key.__init__(self, keys_config)

    def _getKeyNames(self, ktype):
        try:
            return self.list_keys().get(ktype)
        except SystemExit:
            logging.error('Salt terminated trying to retrieve unaccepted keys.')


class RemoteSaltKeyAdapter(BaseSaltKeyAdapter):
    """ Adaptor for remote Salt key management """

    def _getKeyNames(self, ktype):
        remote_salt_key_cmd = get_config().getstring('salt', 'remote_key_command', None)
        output = subprocess.check_output(remote_salt_key_cmd.split(' ') + ['--no-color', '--out=raw'])
        data = eval(output) if output else {}
        return data.get(ktype)


def getKeyAdapter():
    remote_salt_key_cmd = get_config().getstring('salt', 'remote_key_command', None)
    if remote_salt_key_cmd:
        return RemoteSaltKeyAdapter()
    else:
        return SaltKeyAdapter()


class IncomingMachinesSalt(BaseIncomingMachines):
    __name__ = 'salt'

    def _get(self):
        return getKeyAdapter().getUnacceptedKeyNames() or []


class IncomingMachinesSaltInjector(ContainerInjector):
    context(IncomingMachines)
    __class__ = IncomingMachinesSalt


class RegisteredMachinesSalt(object):

    def _get(self):
        return getKeyAdapter().getAcceptedKeyNames() or []


class SaltKeyManager(GlobalUtility):
    implements(IKeyManager)
    name('saltkeymanager')

    def get_accepted_machines(self):
        return RegisteredMachinesSalt()._get() or []

    @defer.inlineCallbacks
    def import_machines(self, accepted):
        for host in accepted:
            yield register_machine(host, mgt_stack=ISaltInstalled)
