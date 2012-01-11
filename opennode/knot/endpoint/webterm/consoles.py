from grokcore.component import context, name

from opennode.oms.endpoint.webterm.root import ConsoleView, SSHClientTerminalProtocol
from opennode.oms.endpoint.webterm.ssh import ssh_connect_interactive_shell

from opennode.knot.model.compute import Computes
from opennode.knot.model.console import ISshConsole, ITtyConsole, IOpenVzConsole


class HypervisorSshTerminalProtocol(SSHClientTerminalProtocol):
    """Connect to a console via ssh on phy + some command"""

    def __init__(self, console):
        phy = console.__parent__.__parent__.__parent__.__parent__
        super(HypervisorSshTerminalProtocol, self).__init__('root', phy.hostname, port=22)
        self.console = console

    def connection_made(self, terminal, size):
        self.transport = terminal.transport
        ssh_connect_interactive_shell(self.user, self.host, self.port, self.transport, self.set_channel, size, self.command)


class TtyTerminalProtocol(HypervisorSshTerminalProtocol):
    """Connect to a tty via ssh on phy + screen."""

    @property
    def command(self):
        return 'screen -xRR %s %s' % (self.console.pty.replace('/', ''), self.console.pty)


class OpenVzTerminalProtocol(HypervisorSshTerminalProtocol):
    """Connect to a openvz console via ssh on phy + vzctl."""

    @property
    def command(self):
        return 'vzctl enter %s' % (self.console.cid)


class SshConsoleView(ConsoleView):
    context(ISshConsole)
    name('webterm')

    @property
    def terminal_protocol(self):
        return SSHClientTerminalProtocol(self.context.user, self.context.hostname)


class ArbitraryHostConsoleView(ConsoleView):
    context(Computes)
    name('webterm')

    def get_terminal_protocol(self, request):
        user = request.args['user'][0]
        host = request.args['host'][0]

        return SSHClientTerminalProtocol(user, host)


class TtyConsoleView(ConsoleView):
    context(ITtyConsole)
    name('webterm')

    @property
    def terminal_protocol(self):
        return TtyTerminalProtocol(self.context)


class OpenVzConsoleView(ConsoleView):
    context(IOpenVzConsole)
    name('webterm')

    @property
    def terminal_protocol(self):
        return OpenVzTerminalProtocol(self.context)
