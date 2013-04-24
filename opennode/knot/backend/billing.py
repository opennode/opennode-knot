from grokcore.component import context, implements, name, GlobalUtility

from opennode.knot.model.compute import IVirtualCompute
from opennode.oms.config import get_config
from opennode.oms.model.model.base import IPreValidateHook
from opennode.oms.model.traversal import traverse1
from opennode.oms.zodb import db


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    name('user-credit-check')
    context(IVirtualCompute)

    @db.ro_transact
    def check(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        if billable_group in map(str, principal.groups):
            profile = traverse1('/home/%s' % principal.id)
            assert profile is not None and profile.has_credit(), \
                    'User %s does not have enough credit' % principal.id
