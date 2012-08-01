import traceback

from datetime import datetime
from twisted.internet import defer
from uuid import uuid5, NAMESPACE_DNS

from certmaster import certmaster

from zope.component import provideSubscriptionAdapter
from zope.interface import implements
from grokcore.component.directive import context

from opennode.knot.model.compute import ICompute
from opennode.knot.model.compute import Compute
from opennode.knot.utils.icmp import ping

from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.config import get_config
from opennode.oms.util import subscription_factory, async_sleep
from opennode.oms.zodb import db
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.endpoint.ssh.detached import DetachedProtocol


class PingCheckAction(Action):
    """Remove request of the host for joining OMS/certmaster"""
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
        if len(self.context.pingcheck) > self.mem_limit:
            del self.context.pingcheck[self.mem_limit:]


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
            except Exception:
                if get_config().getboolean('debug', 'print_exceptions'):
                    traceback.print_exc()

            yield async_sleep(self.interval)

    def log(self, msg):
        import threading
        print "[ping-check] (%s) %s" % (threading.current_thread(), msg)

    @defer.inlineCallbacks
    def ping_check(self):

        @db.ro_transact
        def get_computes():
            res = []

            oms_root = db.get_root()['oms_root']
            res = map(lambda i: (i, i.hostname),
                      filter(ICompute.providedBy,
                             map(follow_symlinks,
                                 oms_root['computes'].listcontent())))

            return res

        ping_actions = []
        for i, hostname in (yield get_computes()):
            action = PingCheckAction(i)
            ping_actions.append((hostname,
                                 action.execute(DetachedProtocol(), object())))

        self.log("Starting ping checks")

        # wait for all async synchronization tasks to finish
        for c, deferred in ping_actions:
            try:
                yield deferred
            except Exception as e:
                self.log("Got exception when pinging compute '%s': %s" % (c, e))
                if get_config().getboolean('debug', 'print_exceptions'):
                    traceback.print_exc()
            else:
                self.log("Pinging was ok for compute: '%s'" % c)

        self.log("Ping check complete")

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
                    self.log("yielding sync")
                    yield self.sync()
                    self.log("sync yielded")
            except Exception:
                if get_config().getboolean('debug', 'print_exceptions'):
                    traceback.print_exc()

            yield async_sleep(self.interval)

    def log(self, msg):
        import threading
        print "[sync] (%s) %s" % (threading.current_thread(), msg)

    @defer.inlineCallbacks
    def sync(self):
        self.log("syncing")

        @defer.inlineCallbacks
        def ensure_machine(host):
            @db.ro_transact
            def check():
                machines = db.get_root()['oms_root']['machines']
                return follow_symlinks(machines['by-name'][host])

            @db.transact
            def update():
                machines = db.get_root()['oms_root']['machines']

                machine = Compute(unicode(host), u'active')
                machine.__name__ = str(uuid5(NAMESPACE_DNS, host))
                machines.add(machine)

            if not (yield check()):
                yield update()

        @defer.inlineCallbacks
        def import_machines():
            cm = certmaster.CertMaster()
            for host in cm.get_signed_certs():
                yield ensure_machine(host)

        yield import_machines()

        @db.ro_transact
        def get_machines():
            res = []

            oms_root = db.get_root()['oms_root']
            for i in [follow_symlinks(i) for i in oms_root['machines'].listcontent()]:
                if ICompute.providedBy(i):
                    res.append((i, i.hostname))

            return res

        sync_actions = []
        from opennode.knot.backend.func.compute import SyncAction
        for i, hostname in (yield get_machines()):
            action = SyncAction(i)
            sync_actions.append((hostname, action.execute(DetachedProtocol(), object())))

        self.log("waiting for background sync tasks")
        # wait for all async synchronization tasks to finish
        for c, deferred in sync_actions:
            try:
                yield deferred
            except Exception as e:
                self.log("Got exception when syncing compute '%s': %s" % (c, e))
                if get_config().getboolean('debug', 'print_exceptions'):
                    traceback.print_exc()
            else:
                self.log("Syncing was ok for compute: '%s'" % c)

        self.log("synced")

provideSubscriptionAdapter(subscription_factory(SyncDaemonProcess), adapts=(Proc,))
