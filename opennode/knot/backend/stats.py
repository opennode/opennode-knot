from datetime import datetime
from grokcore.component import implements, GlobalUtility
from zope.authentication.interfaces import IAuthentication
from zope.component import getAllUtilitiesRegisteredFor
from zope.component import getUtility

import logging
import sys

from opennode.knot.model.user import IUserStatisticsProvider
from opennode.knot.model.user import IUserStatisticsLogger
from opennode.knot.model.compute import IVirtualCompute, IDeployed

from opennode.oms.config import get_config
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.zodb import db


log = logging.getLogger(__name__)


class UserComputeStatisticsAggregator(GlobalUtility):
    implements(IUserStatisticsProvider)

    @db.assert_transact
    def get_computes(self, username):
        computes = db.get_root()['oms_root']['computes']
        user_computes = []
        for compute in map(follow_symlinks, computes.listcontent()):
            if not IVirtualCompute.providedBy(compute):
                continue
            if compute.__owner__ == username and IDeployed.providedBy(compute):
                user_computes.append(compute)
        return user_computes

    @db.assert_transact
    def get_credit(self, username):
        profile = db.get_root()['oms_root']['home'][username]
        if profile:
            return profile.credit
        else:
            log.warning('%s is not found among user profiles under /home!', username)
            return 0

    @db.assert_transact
    def save_vm_stats(self, username, stats):
        profile = db.get_root()['oms_root']['home'][username]
        if profile:
            profile.vm_stats = stats

    @db.transact
    def update(self, username):
        auth = getUtility(IAuthentication)
        p = auth.getPrincipal(username)

        if p is None:
            log.warning('User not found in authentication: %s. Possibly a stale profile record.', username)
            return

        if username is None:
            username = p.id

        billable_group = get_config().getstring('auth', 'billable_group', 'users')

        if billable_group not in p.groups:
            log.debug('User %s is not part of billable group: %s', username, billable_group)
            return

        if type(username) not in (str, unicode):
            username = username.id

        user_computes = self.get_computes(username)

        user_stats = {'num_cores_total': 0,
                      'diskspace_total': 0,
                      'memory_total': 0,
                      'vm_count': len(user_computes)}

        for compute in user_computes:
            try:
                # only account for cores and RAM of the running VMs
                if compute.state == u'active':
                    user_stats['num_cores_total'] += compute.num_cores
                    user_stats['memory_total'] += compute.memory or 0
                user_stats['diskspace_total'] += compute.diskspace.get(u'total') or 0
            except Exception:
                log.error('Error collecting stats from %s', compute, exc_info=sys.exc_info())

        user_stats['timestamp'] = datetime.now()
        user_stats['credit'] = self.get_credit(username)

        self.save_vm_stats(username, user_stats)

        loggers = getAllUtilitiesRegisteredFor(IUserStatisticsLogger)
        for logger in loggers:
            logger.log(username, user_stats)

        log.debug('Statistics update logged for %s', username)
        return user_stats
