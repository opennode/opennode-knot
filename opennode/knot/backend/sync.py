from logging import ERROR
from twisted.internet import defer
from twisted.python import log
from zope.component import provideSubscriptionAdapter, getAllUtilitiesRegisteredFor
from zope.interface import implements

from opennode.knot.backend.syncaction import SyncAction
from opennode.knot.backend.network import SyncIPUsageAction
from opennode.knot.backend.operation import OperationRemoteError
from opennode.knot.backend.operation import IPing
from opennode.knot.model.backend import IKeyManager
from opennode.knot.model.compute import ICompute, IManageable
from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.util import subscription_factory, async_sleep
from opennode.oms.zodb import db


@db.ro_transact
def get_manageable_machines():
    oms_root = db.get_root()['oms_root']
    machines = map(follow_symlinks, oms_root['machines'].listcontent())
    res = [(m, m.hostname) for m in machines
           if ICompute.providedBy(m) and IManageable.providedBy(m)]
    return res


@db.ro_transact
def get_machine_by_uuid(uuid):
    return db.get_root()['oms_root']['machines'][uuid]


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
    def sync(self):
        log.msg('Synchronizing. Machines: %s' % (yield get_manageable_machine_hostnames()), system='sync')
        yield self.gather_machines()
        yield self.execute_ping_tests()
        yield self.gather_ippools()

    @defer.inlineCallbacks
    def cleanup_machines(self, accepted):
        hosts_to_delete = [(host, hostname) for host, hostname in (yield get_manageable_machines())
                           if hostname not in accepted]

        if hosts_to_delete:
            log.msg('Deleting machines: %s' % (hosts_to_delete), system='sync')
            # cleanup machines not known by agents
            yield delete_machines(hosts_to_delete)

    @defer.inlineCallbacks
    def gather_machines(self):
        kml = getAllUtilitiesRegisteredFor(IKeyManager)

        accepted = set()

        for key_manager in kml:
            local_accepted = key_manager.get_accepted_machines()
            log.msg('Local accepted on %s: %s' % (key_manager, local_accepted), system='sync')
            if local_accepted is not None:
                yield key_manager.import_machines(local_accepted)
                accepted = accepted.union(local_accepted)

        log.msg('Hosts accepted: %s' % accepted, system='sync')

        yield self.cleanup_machines(accepted)

    @db.transact
    def set_compute_failure_status(self, uuid, status):
        compute = db.get_root()['oms_root']['machines'][uuid]
        compute.failure = bool(status)

    @db.transact
    def set_compute_suspicious_status(self, uuid, status):
        compute = db.get_root()['oms_root']['machines'][uuid]
        compute.suspicious = bool(status)

    def delete_outstanding_request(self, compute):
        if str(compute) in self.outstanding_requests:
            del self.outstanding_requests[str(compute)]
        else:
            log.msg('Unlock: %s not in outstanding requests: %s'
                    % (str(compute), self.outstanding_requests.keys()), system='sync-unlock')

    def execute_sync_action(self, hostname, compute):
        log.msg("Syncing started: '%s'" % hostname, system='sync')
        syncaction = SyncAction(compute)

        @defer.inlineCallbacks
        def handle_remote_error(ore, c, compute):
            ore.trap(OperationRemoteError)
            if ore.value.remote_tb and get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='sync')
            else:
                log.msg(str(ore.value), system='sync', logLevel=ERROR)
            self.delete_outstanding_request(compute)
            yield self.set_compute_suspicious_status(compute.__name__, True)

        @defer.inlineCallbacks
        def handle_error(e, c, compute):
            e.trap(Exception)
            log.msg("Got exception when syncing compute '%s': %s" % (c, e), system='sync')
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='sync')
            self.delete_outstanding_request(compute)
            yield self.set_compute_suspicious_status((yield db.get(compute, '__name__')), True)

        @defer.inlineCallbacks
        def handle_success(r, c, compute):
            log.msg("Syncing completed: '%s'" % c, system='sync')
            self.delete_outstanding_request(compute)
            yield self.set_compute_suspicious_status((yield db.get(compute, '__name__')), False)

        deferred = syncaction.execute(DetachedProtocol(), object())
        deferred.addCallback(handle_success, hostname, compute)
        deferred.addErrback(handle_remote_error, hostname, compute)
        deferred.addErrback(handle_error, hostname, compute)
        self.outstanding_requests[str(compute)] = deferred

    @defer.inlineCallbacks
    def execute_ping_tests(self):

        @defer.inlineCallbacks
        def handle_remote_error(ore, c, compute):
            ore.trap(OperationRemoteError)
            if ore.value.remote_tb and get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='sync')
            else:
                log.msg(str(ore.value), system='sync', logLevel=ERROR)
            self.delete_outstanding_request(compute)
            yield self.set_compute_failure_status((yield db.get(compute, '__name__')), True)

        @defer.inlineCallbacks
        def handle_error(e, c, compute):
            e.trap(Exception)
            log.msg("Got exception on ping test of '%s': %s" % (c, e), system='sync')
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='sync')
            self.delete_outstanding_request(compute)
            yield self.set_compute_failure_status((yield db.get(compute, '__name__')), True)

        @defer.inlineCallbacks
        def handle_success(r, hostname, compute):
            log.msg("Ping test completed: '%s'" % hostname, system='sync')
            self.delete_outstanding_request(compute)
            self.execute_sync_action(hostname, compute)
            yield self.set_compute_failure_status((yield db.get(compute, '__name__')), False)

        for compute, hostname in (yield get_manageable_machines()):
            if str(compute) not in self.outstanding_requests:
                log.msg('Pinging %s (%s)...' % (hostname, compute), system='sync')
                pingtest = IPing(compute)
                deferred = pingtest.run()
                deferred.addCallback(handle_success, hostname, compute)
                deferred.addErrback(handle_remote_error, hostname, compute)
                deferred.addErrback(handle_error, hostname, compute)
                self.outstanding_requests[str(compute)] = deferred
            else:
                log.msg("Pinging %s skipped: previous test not finished yet" % hostname, system='sync')

    @defer.inlineCallbacks
    def gather_ippools(self):
        @db.ro_transact
        def get_ippools():
            return db.get_root()['oms_root']['ippools']

        log.msg('Syncing IP pools...', system='sync')
        yield SyncIPUsageAction((yield get_ippools())).execute(DetachedProtocol(), object())


provideSubscriptionAdapter(subscription_factory(SyncDaemonProcess), adapts=(Proc,))
