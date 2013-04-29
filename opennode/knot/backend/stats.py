from datetime import datetime
from grokcore.component import implements, GlobalUtility
from zope.component import getAllUtilitiesRegisteredFor

import logging

from opennode.knot.model.user import IUserStatisticsProvider
from opennode.knot.model.user import IUserStatisticsLogger
from opennode.knot.model.compute import IVirtualCompute

from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserComputeStatisticsAggregator(GlobalUtility):
    implements(IUserStatisticsProvider)

    def __init__(self):
        self._statistics = {}

    def get_computes(self, username):
        computes = db.get_root()['oms_root']['computes']
        user_computes = []
        for compute in computes.listcontent():
            if not IVirtualCompute.providedBy(compute):
                continue
            if compute.__owner__ == username:
                user_computes.append(compute)
        return user_computes

    def get_credit(self, username):
        userprofile = db.get_root()['oms_root'][username]
        return userprofile.credit

    @db.ro_transact
    def update(self, username):
        user_computes = self.get_computes(username)

        user_stats = {'num_cores_total': 0,
                      'disksize_total': 0,
                      'memory_total': 0,
                      'vm_count': len(user_computes)}

        for compute in user_computes:
            user_stats['num_cores_total'] += compute.num_cores
            user_stats['memory_total'] += compute.memory
            user_stats['disksize_total'] += compute.disksize[u'total']

        user_stats['timestamp'] = datetime.now()
        user_stats['credit'] = self.get_credit(username)
        self._statistics[username] = user_stats

        loggers = getAllUtilitiesRegisteredFor(IUserStatisticsLogger)
        for logger in loggers:
            logger.log(username, user_stats)

        return user_stats

    def get_user_statistics(self, username):
        return self._statistics[username]
