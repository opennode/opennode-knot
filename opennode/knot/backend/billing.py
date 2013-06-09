from datetime import datetime, timedelta
from grokcore.component import implements, name, GlobalUtility
from twisted.internet import defer
from twisted.enterprise import adbapi
from zope.interface import Interface
from zope.component import getUtility

import logging
import sys

from opennode.knot.model.compute import IVirtualCompute
from opennode.knot.model.user import IUserStatisticsLogger
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer

from opennode.oms.config import get_config
from opennode.oms.model.model.hooks import IPreValidateHook
from opennode.oms.model.traversal import traverse1
from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserStatsLogger(GlobalUtility):
    implements(IUserStatisticsLogger)
    name('user-stats-logger')

    def __init__(self):
        self.slog = logging.getLogger("%s.userstats" % __name__)

    def log(self, user, stats_data):
        data = {'username': user, 'stats': stats_data}
        self.slog.info('', extra=data)


class SqlDBUserStatsLogger(GlobalUtility):
    implements(IUserStatisticsLogger)
    name('user-stats-sqldb-logger')

    def initdb(self):
        op = get_config().getstring('stats', 'db_init')
        return self._db.runOperation(op)

    def config(self):
        self.db_backend = get_config().getstring('stats', 'db_backend', 'sqlite3')
        self.db_conn_param = get_config().getstring('stats', 'db_conn_param', ':memory:').split(';')
        self.db_conn_kw = eval(get_config().getstring('stats', 'db_conn_kw', '{}'))
        self.db_operation = get_config().getstring('stats', 'db_operation',
                                                   'INSERT INTO CONF_CHANGES (username, timestamp, cores,'
                                                   'disk, memory, number_of_vms, last_known_credit) '
                                                   'VALUES (%s, %s, %s, %s, %s, %s, %s)')

        self._db = adbapi.ConnectionPool(self.db_backend, *self.db_conn_param, **self.db_conn_kw)

        if get_config().getstring('stats', 'db_init', None):
            return self.initdb()

        return defer.succeed(None)

    @defer.inlineCallbacks
    def log(self, user, stats_data):
        fail = True
        retries = 1
        while fail and retries > 0:
            retries -= 1
            try:
                if not hasattr(self, '_db'):
                    yield self.config()

                logdata = {'user': user}
                logdata.update(stats_data)

                log.debug('writing stats: %(user)s numcores: %(num_cores_total)s disk: %(diskspace_total)s '
                          'memory: %(memory_total)s vmcount: %(vm_count)s credit: %(credit)s', logdata)

                yield self._db.runOperation(self.db_operation,
                                            (user, stats_data['timestamp'],
                                             stats_data['num_cores_total'],
                                             # OMS_USAGE db assumes GBs for
                                             # memory while as OMS internally calculates in MBs
                                             stats_data['diskspace_total'],
                                             stats_data['memory_total'] / 1024.0,
                                             stats_data['vm_count'],
                                             stats_data['credit']))
                fail = False
            except Exception:
                log.error('DB error', exc_info=sys.exc_info())
                fail = True


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    name('user-credit-check')

    @defer.inlineCallbacks
    def apply(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        credit_check_cooldown = get_config().getstring('auth', 'billing_timeout', 60)

        @db.ro_transact
        def get_profile_and_need_update():
            try:
                profile = traverse1('/home/%s' % principal.id)
                timeout = (datetime.strptime(profile.credit_timestamp, '%Y-%m-%dT%H:%M:%S.%f') +
                           timedelta(seconds=credit_check_cooldown))
                log.debug('Next update for "%s": %s', principal.id, timeout)
                return (profile, profile.uid, timeout < datetime.now())
            except Exception as e:
                log.error('%s', e)
                raise

        @db.transact
        def update_credit(credit, balance_limit):
            profile = traverse1('/home/%s' % principal.id)
            profile.credit = credit
            profile.balance_limit = balance_limit
            log.debug('Updated credit of %s: %s, %s', profile, profile.credit, profile.balance_limit)
            return profile

        if billable_group in map(str, principal.groups):
            profile, uid, need_update = yield get_profile_and_need_update()
            log.debug('%s (uid=%s) need_update: %s', profile, uid, need_update)

            if need_update:
                try:
                    check_call = getUtility(ICreditCheckCall)
                    credit, balance_limit = yield defer.maybeDeferred(check_call.get_credit, uid)
                    profile = yield update_credit(credit, balance_limit)
                except Exception as e:
                    log.error('Error updating credit: %s', e, exc_info=sys.exc_info())

            @db.ro_transact()
            def check_credit(profile):
                log.debug('Checking if user %s has credit (%s): %s (%s)',
                          profile, profile.credit, profile.has_credit(), profile.credit > 0 - profile.balance_limit)
                assert profile.has_credit(), ('User %s does not have enough credit' % principal.id)

            yield check_credit(profile)
        else:
            log.info('User "%s" is not a member of a billable group "%s": %s. Not updating credit.',
                     principal.id, billable_group, map(str, principal.groups))

    def applicable(self, context):
        return IVirtualCompute.providedBy(context) or IVirtualizationContainer.providedBy(context)


class ICreditCheckCall(Interface):

    def get_credit(self, uid):
        """ Get credit """
