# Copyright (c) 2012 Alan Franzoni
# Licensed under Apache license 2.0
# Published: http://code.activestate.com/recipes/578021-async-subprocess-check_output-replacement-for-twis/
from twisted.internet.defer import Deferred
from twisted.internet.error import ProcessDone
from twisted.internet.protocol import ProcessProtocol

class SubprocessProtocol(ProcessProtocol):
    outBuffer = ""
    errBuffer = ""

    def connectionMade(self):
        self.d = Deferred()

    def outReceived(self, data):
        self.outBuffer += data

    def errReceived(self, data):
        self.errBuffer += data

    def processEnded(self, reason):
        if reason.check(ProcessDone):
            self.d.callback(self.outBuffer)
        else:
            self.d.errback(reason)

def async_check_output(args, ireactorprocess=None):
    """
    :type args: list of str
    :type ireactorprocess: :class: twisted.internet.interfaces.IReactorProcess
    :rtype: Deferred
    """
    if ireactorprocess is None:
        from twisted.internet import reactor
        ireactorprocess = reactor

    pprotocol = SubprocessProtocol()
    ireactorprocess.spawnProcess(pprotocol, args[0], args)
    return pprotocol.d
