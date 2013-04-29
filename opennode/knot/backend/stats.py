from grokcore.component import implements, GlobalUtility
from twisted.internet import defer

import logging
from datetime import datetime

from opennode.knot.model.user import IUserStatisticsProvider
from opennode.knot.model.compute import IVirtualCompute

from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserComputeStatisticsAggregator(GlobalUtility):
    implements(IUserStatisticsProvider)

    def __init__(self):
        self._statistics = {}

    @db.ro_transact
    def get_user_computes(self, username):
        computes = db.get_root()['oms_root']['computes']
        user_computes = []
        for compute in computes.listcontent():
            if not IVirtualCompute.providedBy(compute):
                continue
            if compute.__owner__ == username:
                user_computes.append(compute)
        return user_computes

    @defer.inlineCallbacks
    def update(self, username):
        user_computes = yield self.get_user_computes(username)

        user_stats = {'timestamp': datetime.now(),
                      'num_cores_total': 0,
                      'disksize_total': 0,
                      'memory_total': 0,
                      'vm_count': len(user_computes)}

        self._statistics[username] = user_stats

        for compute in user_computes:
            user_stats['num_cores_total'] += compute.num_cores
            user_stats['memory_total'] += compute.memory
            user_stats['disksize_total'] += compute.disksize[u'total']

        user_stats['timestamp'] = datetime.now()
        defer.returnValue(user_stats)

    def get_user_statistics(self, username):
        return self._statistics[username]
