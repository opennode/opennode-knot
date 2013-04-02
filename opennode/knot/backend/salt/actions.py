
from grokcore.component import context, implements
from twisted.internet import defer
from twisted.python import log

from opennode.knot.backend.operation import IInstallPkg
from opennode.knot.backend.compute import ComputeAction
from opennode.knot.model.compute import ICompute, IVirtualCompute

from opennode.oms.endpoint.ssh.cmd.base import Cmd
from opennode.oms.endpoint.ssh.cmd.directives import command
from opennode.oms.endpoint.ssh.cmdline import ICmdArgumentsSyntax
from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.model.actions import action
from opennode.oms.zodb import db


class InstallSaltAction(ComputeAction):
    context(ICompute)
    action('install-salt')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context.__parent__,))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        if IVirtualCompute.providedBy(self.context):
            cmd.write('Only applicable to HNs\n')
            return

        log.msg('Installing salt-minion...', system='install-salt')
        yield IInstallPkg(self.context).run('salt-minion')
        log.msg('Installation of salt-minion successful!', system='install-salt')


class InstallSaltCmd(Cmd):
    implements(ICmdArgumentsSyntax)
    command('install-salt')

    def arguments(self):
        return VirtualConsoleArgumentParser()

    @db.ro_transact(proxy=False)
    def subject(self, args):
        return tuple(self.traverse(path) for path in args.paths)

    @db.transact
    def execute(self, args):
        for machine in db.get_root()['oms_root']['machines']:
            if not ICompute.providedBy(machine) or IVirtualCompute.providedBy(machine):
                continue

            InstallSaltAction(machine).execute(DetachedProtocol(), object())
