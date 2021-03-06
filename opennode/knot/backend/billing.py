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
        self.slog.info('%s' % data, extra=data)


class SqlDBUserStatsLogger(GlobalUtility):
    implements(IUserStatisticsLogger)
    name('user-stats-sqldb-logger')

    def initdb(self):
        op = get_config().getstring('stats', 'db_init')
        return self._db.runOperation(op)

    def config(self):
        if not get_config().getboolean('stats', 'enabled', True):
            return defer.succeed(None)

        self.db_backend = get_config().getstring('stats', 'db_backend', 'sqlite3')
        self.db_conn_param = get_config().getstring('stats', 'db_conn_param', ':memory:').split(';')
        self.db_conn_kw = eval(get_config().getstring('stats', 'db_conn_kw', '{}'))
        self.db_operation = get_config().getstring('stats', 'db_operation',
                                                   'INSERT INTO CONF_CHANGES (username, timestamp, cores,'
                                                   'disk, memory, number_of_vms, last_known_credit) '
                                                   'VALUES (%s, %s, %s, %s, %s, %s, %s)')

        self._db = adbapi.ConnectionPool(self.db_backend, *self.db_conn_param, **self.db_conn_kw)
        self._db.noisy = get_config().getboolean('debug', 'stats_debug', False)

        if get_config().getstring('stats', 'db_init', None):
            return self.initdb()

        return defer.succeed(None)

    @defer.inlineCallbacks
    def log(self, user, stats_data):
        try:
            if not hasattr(self, '_db'):
                yield self.config()
                if getattr(self, 'db_backend', None) is None:
                    return

            logdata = {'user': user if type(user) in (str, unicode) else user.id}
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

            log.debug('writing stats for %(user)s is done!', logdata)
        except Exception:
            log.error('DB error', exc_info=sys.exc_info())


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    name('user-credit-check')

    @db.ro_transact
    def _get_profile_and_need_update(self, principal):
        credit_check_cooldown = get_config().getstring('auth', 'billing_timeout', 60)
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
    def _update_credit(self, principal, credit, balance_limit):
        profile = traverse1('/home/%s' % principal.id)
        profile.credit = credit
        profile.balance_limit = balance_limit
        log.debug('Updated credit of %s: %s, %s', profile, profile.credit, profile.balance_limit)
        return profile

    @defer.inlineCallbacks
    def apply(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        if billable_group in map(str, principal.groups):
            profile, uid, need_update = yield self._get_profile_and_need_update(principal)
            log.debug('Need update %s uid=%s : %s', profile, uid, need_update)

            if need_update:
                try:
                    check_call = getUtility(ICreditCheckCall)
                    credit, balance_limit = yield defer.maybeDeferred(check_call.get_credit, uid)
                    profile = yield self._update_credit(principal, credit, balance_limit)
                except Exception as e:
                    log.error('Error updating credit: %s', e, exc_info=sys.exc_info())

            @db.ro_transact()
            def check_credit(profile):
                log.debug('Checking if user %s has credit (%s): %s',
                          profile, profile.credit, profile.has_credit())
                assert profile.has_credit(), 'User %s does not have enough credit' % principal.id

            yield check_credit(profile)
        else:
            log.info('User "%s" is not a member of a billable group "%s": %s. Not updating credit',
                     principal.id, billable_group, map(str, principal.groups))

    def applicable(self, context):
        return any(map(lambda subj: subj.providedBy(context),
                       (IVirtualCompute, IVirtualizationContainer)))


class ICreditCheckCall(Interface):

    def get_credit(self, uid):
        """ Get credit """
