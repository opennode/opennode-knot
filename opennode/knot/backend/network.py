from grokcore.component import context
from twisted.internet import defer
from twisted.python import log
from netaddr import IPAddress

from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.endpoint.ssh.cmd.security import require_admins_only_action
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.zodb import db

from opennode.knot.model.compute import ICompute
from opennode.knot.model.network import IPv4Pools, IPv4Pool


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
                    ip = IPAddress(c.ipv4_address.split('/')[0])
                    pool = pools.find_pool(ip)
                    if pool is not None and not pool.get(ip):
                        log.msg('Marking %s as used...' % ip, system='sync-ippool')
                        pool.use(ip)
            except Exception:
                log.err(system='sync-ippool')
                raise
        yield get_compute_ips()


class ManageIpAction(Action):
    context(IPv4Pool)
    action('manage')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context, ))

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('-f', '--free', action='store_true', help='Mark IP as free')
        parser.add_argument('-u', '--use', action='store_true', help='Mark IP as used')
        parser.add_argument('ip')
        return parser

    @require_admins_only_action
    @defer.inlineCallbacks
    def execute(self, cmd, args):
        @db.transact
        def manage(cmd, args):
            ip = IPAddress(args.ip)
            if args.free:
                self.context.free(ip)
            elif args.use:
                self.context.use(ip)
        yield manage(cmd, args)
