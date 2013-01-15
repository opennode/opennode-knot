from __future__ import absolute_import
import logging

from grokcore.component import context, implements, name, GlobalUtility
import salt.config
from salt.key import Key
from salt.utils import parsers
from twisted.internet import defer

from opennode.knot.model.backend import IKeyManager
from opennode.knot.backend.compute import register_machine
from opennode.knot.model.machines import IncomingMachines, BaseIncomingMachines
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.model.model.base import  ContainerInjector


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


class SaltKeyAdapter(Key, parsers.ConfigDirMixIn):
    """ Adaptor for Salt key management logic """

    def __init__(self):
        self.options = DummyOptions()
        keys_config = salt.config.master_config(self.get_config_file_path('master'))
        Key.__init__(self, keys_config)
        self.REJECTED = 'minions_rejected'
        self.ACCEPTED = 'minions'
        self.UNACCEPTED = 'minions_pre'

    def _getKeyNames(self, ktype):
        try:
            return self.list_keys()[ktype]
        except SystemExit:
            logging.error('Salt terminated trying to retrieve unaccepted keys.')

    def getUnacceptedKeyNames(self):
        return self._getKeyNames(self.UNACCEPTED)

    def getAcceptedKeyNames(self):
        return self._getKeyNames(self.ACCEPTED)

    def getRejectedKeyNames(self):
        return self._getKeyNames(self.REJECTED)


class IncomingMachinesSalt(BaseIncomingMachines):
    __name__ = 'salt'

    def _get(self):
        return SaltKeyAdapter().getUnacceptedKeyNames()


class IncomingMachinesSaltInjector(ContainerInjector):
    context(IncomingMachines)
    __class__ = IncomingMachinesSalt


class RegisteredMachinesSalt(object):
    def _get(self):
        return SaltKeyAdapter().getAcceptedKeyNames()


class SaltKeyManager(GlobalUtility):
    implements(IKeyManager)
    name('saltkeymanager')

    def get_accepted_machines(self):
        return RegisteredMachinesSalt()._get()

    @defer.inlineCallbacks
    def import_machines(self, accepted):
        for host in accepted:
            yield register_machine(host, mgt_stack=ISaltInstalled)

