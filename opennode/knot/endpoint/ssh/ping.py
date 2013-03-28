from grokcore.component import implements
from zope.component import provideSubscriptionAdapter

from opennode.knot.utils.icmp import ping
from opennode.knot.model.compute import ICompute
from opennode.oms.endpoint.ssh.cmd.base import Cmd
from opennode.oms.endpoint.ssh.cmd.completers import PathCompleter
from opennode.oms.endpoint.ssh.cmd.directives import command
from opennode.oms.endpoint.ssh.cmdline import ICmdArgumentsSyntax, VirtualConsoleArgumentParser
from opennode.oms.zodb import db


class PingCmd(Cmd):
    implements(ICmdArgumentsSyntax)
    command('ping')

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('host', nargs='+', help="Host or compute object")
        return parser

    @db.transact
    def execute(self, args):
        for arg in args.host:
            obj = self.traverse(arg)
            if ICompute.providedBy(obj):
                address = obj.hostname.encode('utf-8')
            else:
                address = arg
            res = ping(address)
            self.write("%s is %s\n" % (address, ["unreachable", "alive"][res]))


for cmd in [PingCmd]:
    provideSubscriptionAdapter(PathCompleter, adapts=(cmd, ))
