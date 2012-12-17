from datetime import datetime
from grokcore.component.directive import context
from twisted.internet import defer
from twisted.python import log, failure
from zope.component import provideSubscriptionAdapter
from zope.interface import implements
import logging
import traceback

from opennode.knot.backend.compute import SyncAction
from opennode.knot.backend import func as func_backend
from opennode.knot.backend import salt as salt_backend
from opennode.knot.model.compute import ICompute
from opennode.knot.model.compute import IManageable
from opennode.knot.utils.icmp import ping
from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.util import subscription_factory, async_sleep
from opennode.oms.zodb import db


class PingCheckAction(Action):
    """Check if a Compute responds to ICMP request from OMS."""
    context(ICompute)

    action('ping-check')

    def __init__(self, *args, **kwargs):
        super(PingCheckAction, self).__init__(*args, **kwargs)
        config = get_config()
        self.mem_limit = config.getint('pingcheck', 'mem_limit')

    @defer.inlineCallbacks
    def execute(self, cmd, args):
        yield self._execute(cmd, args)

    @db.transact
    def _execute(self, cmd, args):
        address = self.context.hostname.encode('utf-8')
        res = ping(address)
        self.context.last_ping = (res == 1)
        self.context.pingcheck.append({'timestamp': datetime.utcnow(),
                                       'result': res})
        history_len = len(self.context.pingcheck)
        if history_len > self.mem_limit:
            del self.context.pingcheck[:-self.mem_limit]

        ping_results = map(lambda i: i['result'] == 1, self.context.pingcheck[:3])

        self.context.suspicious = not all(ping_results)
        self.context.failure = not any(ping_results)


class PingCheckDaemonProcess(DaemonProcess):
    implements(IProcess)

    __name__ = "ping-check"

    def __init__(self):
        super(PingCheckDaemonProcess, self).__init__()

        config = get_config()
        self.interval = config.getint('pingcheck', 'interval')

    @defer.inlineCallbacks
    def run(self):
        while True:
            try:
                if not self.paused:
                    yield self.ping_check()
            except Exception as e:
                log.err(e, system='sync')
                if get_config().getboolean('debug', 'print_exceptions'):
                    log.err(traceback.format_exc(e), system='sync')

            yield async_sleep(self.interval)

    @defer.inlineCallbacks
    def ping_check(self):

        @db.ro_transact
        def get_computes():
            oms_root = db.get_root()['oms_root']
            res = [(i, i.hostname)
                   for i in map(follow_symlinks,
                                oms_root['computes'].listcontent())
                   if ICompute.providedBy(i)]

            return res

        ping_actions = []
        for i, hostname in (yield get_computes()):
            action = PingCheckAction(i)
            ping_actions.append((hostname,
                                 action.execute(DetachedProtocol(), object())))

        # wait for all async synchronization tasks to finish
        for c, deferred in ping_actions:
            try:
                yield deferred
            except Exception as e:
                log.msg("Got exception when pinging compute '%s': %s" % (c, e), system='ping-check')
                if get_config().getboolean('debug', 'print_exceptions'):
                    log.err(e, system='ping-check')


provideSubscriptionAdapter(subscription_factory(PingCheckDaemonProcess), adapts=(Proc,))


class SyncDaemonProcess(DaemonProcess):
    implements(IProcess)

    __name__ = "sync"

    def __init__(self):
        super(SyncDaemonProcess, self).__init__()

        config = get_config()
        self.interval = config.getint('sync', 'interval')

    @defer.inlineCallbacks
    def run(self):
        while True:
            try:
                if not self.paused:
                    log.msg("yielding sync", system='sync', logLevel=logging.DEBUG)
                    yield self.sync()
                    log.msg("sync yielded", system='sync', logLevel=logging.DEBUG)
            except Exception, e:
                if get_config().getboolean('debug', 'print_exceptions'):
                    log.err(e, system='sync')

            yield async_sleep(self.interval)

    @defer.inlineCallbacks
    def sync(self):
        log.msg("syncing", system='sync')

        # TODO: decouple via interfaces?
        yield salt_backend.machines.import_machines()
        yield func_backend.machines.import_machines()

        sync_actions = (yield self._getSyncActions())

        log.msg("waiting for background sync tasks", system='sync', logLevel=logging.DEBUG)

        # wait for all async synchronization tasks to finish
        for c, deferred in sync_actions:
            try:
                yield deferred
            except Exception as e:
                log.msg("Got exception when syncing compute '%s': %s" % (c, e), system='sync')
                if get_config().getboolean('debug', 'print_exceptions'):
                    log.err(failure.Failure(e), system='sync')
            else:
                log.msg("Syncing was ok for compute: '%s'" % c, system='sync')

        log.msg("synced", system='sync', logLevel=logging.DEBUG)

    @defer.inlineCallbacks
    def _getSyncActions(self):
        @db.ro_transact
        def get_machines():
            oms_root = db.get_root()['oms_root']
            res = [(i, i.hostname) for i in
                   (follow_symlinks(j) for j in oms_root['machines'].listcontent())
                    if ICompute.providedBy(i)]
            return res

        sync_actions = []

        for i, hostname in (yield get_machines()):
            if IManageable.providedBy(i):
                action = SyncAction(i)
                sync_actions.append((hostname, action.execute(DetachedProtocol(), object())))

        defer.returnValue(sync_actions)

provideSubscriptionAdapter(subscription_factory(SyncDaemonProcess), adapts=(Proc,))
