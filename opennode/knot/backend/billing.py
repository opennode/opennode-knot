from datetime import datetime, timedelta
from grokcore.component import context, implements, name, GlobalUtility
from twisted.internet import defer
from zope.interface import Interface
from zope.component import getUtility

import logging
import sys

from opennode.knot.model.compute import IVirtualCompute
from opennode.knot.model.user import IUserStatisticsLogger

from opennode.oms.config import get_config
from opennode.oms.model.model.hooks import IPreValidateHook
from opennode.oms.model.traversal import traverse1
from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserStatsLogger(GlobalUtility):
    implements(IUserStatisticsLogger)

    def log(self, user, data):
        slog = logging.getLogger("%s.userstats" % __name__)
        slog.info("%s, %s", user, data)


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    context(IVirtualCompute)
    name('user-credit-check')

    @defer.inlineCallbacks
    def apply(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        credit_check_cooldown = get_config().getstring('auth', 'billing_timeout', 60)

        @db.ro_transact
        def get_profile_and_need_update():
            try:
                profile = traverse1('/home/%s' % principal.id)
                log.debug('Next update for "%s": %s', principal.id,
                          datetime.strptime(profile.credit_timestamp, '%Y-%m-%dT%H:%M:%S.%f') +
                          timedelta(seconds=credit_check_cooldown))
                return (profile, profile.uid,
                        (datetime.strptime(profile.credit_timestamp, '%Y-%m-%dT%H:%M:%S.%f') +
                         timedelta(seconds=credit_check_cooldown)) < datetime.now())
            except Exception as e:
                log.error('%s', e)
                raise

        @db.transact
        def update_credit(credit):
            profile = traverse1('/home/%s' % principal.id)
            profile.credit = credit
            log.debug('Updated credit of %s: %s', profile, profile.credit)
            return profile

        log.debug('%s in %s: %s', billable_group,
                  map(str, principal.groups), billable_group in map(str, principal.groups))

        if billable_group in map(str, principal.groups):
            profile, uid, need_update = yield get_profile_and_need_update()
            log.debug('%s (uid=%s) need_update: %s', profile, uid, need_update)

            if need_update:
                try:
                    check_call = getUtility(ICreditCheckCall)
                    credit = yield defer.maybeDeferred(check_call.get_credit, uid)
                    profile = yield update_credit(credit)
                except Exception as e:
                    log.error('Error updating credit: %s', e, exc_info=sys.exc_info())

            @db.ro_transact()
            def check_credit(profile):
                assert profile.has_credit(), ('User %s does not have enough credit' % principal.id)

            yield check_credit(profile)

    def applicable(self, context):
        return IVirtualCompute.providedBy(context)


class ICreditCheckCall(Interface):

    def get_credit(self, uid):
        """ Get credit """
