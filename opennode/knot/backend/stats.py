from datetime import datetime
from grokcore.component import implements, GlobalUtility
from zope.authentication.interfaces import IAuthentication
from zope.component import getAllUtilitiesRegisteredFor
from zope.component import getUtility

import logging
import sys

from opennode.knot.model.user import IUserStatisticsProvider
from opennode.knot.model.user import IUserStatisticsLogger
from opennode.knot.model.compute import IVirtualCompute

from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserComputeStatisticsAggregator(GlobalUtility):
    implements(IUserStatisticsProvider)

    def __init__(self):
        self._statistics = {}

    def get_computes(self, username):
        computes = db.get_root()['oms_root']['computes']
        user_computes = []
        for compute in map(follow_symlinks, computes.listcontent()):
            if not IVirtualCompute.providedBy(compute):
                continue
            if compute.__owner__ == username:
                user_computes.append(compute)
        return user_computes

    def get_credit(self, username):
        profile = db.get_root()['oms_root']['home'][username]
        if profile:
            return profile.credit
        else:
            log.warning('%s is not found among user profiles under /home!', username)
            return 0

    @db.ro_transact
    def update(self, username):
        if username is None:
            auth = getUtility(IAuthentication)
            p = auth.getPrincipal(username)
            username = p.id

        user_computes = self.get_computes(username)

        user_stats = {'num_cores_total': 0,
                      'diskspace_total': 0,
                      'memory_total': 0,
                      'vm_count': len(user_computes)}

        log.debug('%s computes of user %s', len(user_computes), username)

        for compute in user_computes:
            try:
                user_stats['num_cores_total'] += compute.num_cores
                user_stats['memory_total'] += compute.memory or 0
                user_stats['diskspace_total'] += compute.diskspace.get(u'total') or 0
            except Exception:
                log.error('Error collecting stats from %s', compute, exc_info=sys.exc_info())

        user_stats['timestamp'] = datetime.now()
        user_stats['credit'] = self.get_credit(username)
        self._statistics[username] = user_stats

        loggers = getAllUtilitiesRegisteredFor(IUserStatisticsLogger)
        for logger in loggers:
            logger.log(username, user_stats)

        return user_stats

    def get_user_statistics(self, username):
        return self._statistics[username]
