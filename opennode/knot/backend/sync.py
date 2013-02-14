from datetime import datetime
from grokcore.component.directive import context
from logging import ERROR
from twisted.internet import defer
from twisted.python import log
from zope.component import provideSubscriptionAdapter, getAllUtilitiesRegisteredFor
from zope.interface import implements

from opennode.knot.backend.compute import SyncAction
from opennode.knot.backend.operation import OperationRemoteError
from opennode.knot.model.backend import IKeyManager
from opennode.knot.model.compute import ICompute, IManageable
from opennode.knot.utils.icmp import ping
from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.util import subscription_factory, async_sleep, timeout
from opennode.oms.zodb import db


class PingCheckAction(Action):
    """Check if a Compute responds to ICMP request from OMS."""
    context(ICompute)

    action('ping-check')

    def __init__(self, *args, **kwargs):
        super(PingCheckAction, self).__init__(*args, **kwargs)
        config = get_config()
        self.mem_limit = config.getint('pingcheck', 'mem_limit')

    @db.ro_transact(proxy=False)
    def subject(self, args):
        return tuple((self.context, ))

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
        self.outstanding_requests = {}

    @defer.inlineCallbacks
    def run(self):
        while True:
            try:
                if not self.paused:
                    yield self.ping_check()
            except Exception:
                if get_config().getboolean('debug', 'print_exceptions'):
                    log.err(system='ping-check')

            yield async_sleep(self.interval)

    @defer.inlineCallbacks
    def ping_check(self):

        @db.ro_transact
        def get_computes():
            oms_root = db.get_root()['oms_root']
            res = [(i, i.hostname)
                   for i in map(follow_symlinks, oms_root['computes'].listcontent())
                   if ICompute.providedBy(i)]

            return res

        ping_actions = []
        for i, hostname in (yield get_computes()):
            action = PingCheckAction(i)
            d = timeout(self.interval * 3)(action.execute)(DetachedProtocol(), object())
            self.outstanding_requests[hostname] = d
            ping_actions.append((hostname, d))

        def handle_success(r, c):
            del self.outstanding_requests[c]

        def handle_errors(e, c):
            e.trap(Exception)
            log.msg("Got exception when pinging compute '%s': %s" % (c, e), system='ping-check')
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='ping-check')
            del self.outstanding_requests[c]

        for c, deferred in ping_actions:
            deferred.addCallback(handle_success, c)
            deferred.addErrback(handle_errors, c)

provideSubscriptionAdapter(subscription_factory(PingCheckDaemonProcess), adapts=(Proc,))


@db.ro_transact
def get_manageable_machines():
    oms_root = db.get_root()['oms_root']
    machines = map(follow_symlinks, oms_root['machines'].listcontent())
    res = [(m, m.hostname) for m in machines
           if ICompute.providedBy(m) and IManageable.providedBy(m)]
    return res


@db.transact
def delete_machines(delete_list):
    oms_machines = db.get_root()['oms_root']['machines']

    for host, hostname in delete_list:
        del oms_machines[host.__name__]


@defer.inlineCallbacks
def get_manageable_machine_hostnames():
    defer.returnValue(map(lambda h: h[0].hostname, (yield get_manageable_machines())))


class SyncDaemonProcess(DaemonProcess):
    implements(IProcess)

    __name__ = "sync"

    def __init__(self):
        super(SyncDaemonProcess, self).__init__()

        config = get_config()
        self.interval = config.getint('sync', 'interval')
        self.outstanding_requests = {}

    @defer.inlineCallbacks
    def run(self):
        while True:
            try:
                if not self.paused:
                    yield self.sync()
            except Exception:
                if get_config().getboolean('debug', 'print_exceptions'):
                    log.err(system='sync')

            yield async_sleep(self.interval)

    @defer.inlineCallbacks
    def cleanup(self, accepted):
        hosts_to_delete = [(host, hostname) for host, hostname in (yield get_manageable_machines())
                           if hostname not in accepted]

        if hosts_to_delete:
            log.msg('Deleting hosts: %s' % (hosts_to_delete), system='sync')
            # cleanup machines not known by agents
            yield delete_machines(hosts_to_delete)

    @defer.inlineCallbacks
    def sync(self):
        log.msg('Synchronizing. Machines: %s' % (yield get_manageable_machine_hostnames()), system='sync')

        kml = getAllUtilitiesRegisteredFor(IKeyManager)

        accepted = set()

        for key_manager in kml:
            local_accepted = key_manager.get_accepted_machines()
            log.msg('Local accepted on %s: %s' % (key_manager, local_accepted), system='sync')
            if local_accepted is not None:
                yield key_manager.import_machines(local_accepted)
                accepted = accepted.union(local_accepted)

        log.msg('Hosts accepted: %s' % accepted, system='sync')

        yield self.cleanup(accepted)

        sync_actions = (yield self._getSyncActions())

        def handle_remote_error(ore, c):
            ore.trap(OperationRemoteError)
            if ore.value.remote_tb and get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='sync')
            else:
                log.msg(str(ore.value), system='sync', logLevel=ERROR)
            del self.outstanding_requests[c]

        def handle_error(e, c):
            e.trap(Exception)
            log.msg("Got exception when syncing compute '%s': %s" % (c, e), system='sync')
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='sync')
            del self.outstanding_requests[c]

        def handle_success(r, c):
            log.msg("Syncing completed: '%s'" % c, system='sync')
            del self.outstanding_requests[c]

        for c, deferred in sync_actions:
            deferred.addCallback(handle_success, c)
            deferred.addErrback(handle_remote_error, c)
            deferred.addErrback(handle_error, c)

    @defer.inlineCallbacks
    def _getSyncActions(self):
        sync_actions = []
        for i, hostname in (yield get_manageable_machines()):
            if hostname not in self.outstanding_requests:
                action = SyncAction(i)
                log.msg("Syncing started: '%s'" % hostname, system='sync')
                deferred = timeout(self.interval * 3)(action.execute)(DetachedProtocol(), object())
                self.outstanding_requests[hostname] = deferred
                sync_actions.append((hostname, deferred))
            else:
                log.msg("Syncing %s skipped: previous request not finished yet" % hostname, system='sync')

        defer.returnValue(sync_actions)

provideSubscriptionAdapter(subscription_factory(SyncDaemonProcess), adapts=(Proc,))
