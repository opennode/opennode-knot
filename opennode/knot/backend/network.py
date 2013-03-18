from grokcore.component import context
from twisted.internet import defer
from twisted.python import log

from opennode.knot.model.compute import ICompute
from opennode.knot.model.network import IPv4Pools, IPv4Address
from opennode.oms.model.model.actions import Action
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.zodb import db


class SyncIPUsageAction(Action):
    context(IPv4Pools)

    @db.ro_transact
    def subject(self):
        return tuple((self.context, ))

    @defer.inlineCallbacks
    def execute(self, cmd, args):
        @db.transact
        def get_compute_ips():
            try:
                computes = map(follow_symlinks,
                               filter(lambda c: ICompute.providedBy(follow_symlinks(c)),
                                  db.get_root()['oms_root']['computes'].listcontent()))
                pools = db.get_root()['oms_root']['ippools']
                for c in computes:
                    ip = IPv4Address(c.ipv4_address.split('/')[0])
                    pool = pools.find_pool(ip)
                    if pool is not None and not pool.get(ip):
                        log.msg('Marking %s as used...' % ip, system='sync-ippool')
                        pool.use(ip)
            except Exception:
                log.err(system='sync-ippool')
                raise
        yield get_compute_ips()
