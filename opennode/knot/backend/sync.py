from datetime import datetime
from logging import ERROR
from twisted.internet import defer
from twisted.python import log
from zope.authentication.interfaces import IAuthentication
from zope.component import provideSubscriptionAdapter, getAllUtilitiesRegisteredFor
from zope.component import getUtility
from zope.interface import implements

from opennode.knot.backend.syncaction import SyncAction
from opennode.knot.backend.network import SyncIPUsageAction
from opennode.knot.backend.operation import OperationRemoteError
from opennode.knot.backend.operation import IPing
from opennode.knot.model.backend import IKeyManager
from opennode.knot.model.compute import ICompute, IManageable
from opennode.knot.model.user import UserProfile
from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.security.principals import User
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


def set_compute_failure_status(uuid, status):
    compute = db.get_root()['oms_root']['machines'][uuid]
    compute.failure = bool(status)

    def iterate_recursively(container):
        seen = set()
        for item in container.listcontent():
            if ICompute.providedBy(item):
                item.failure = bool(status)

            from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
            if (IVirtualizationContainer.providedBy(item) or ICompute.providedBy(item)):
                if item.__name__ not in seen:
                    seen.add(item.__name__)
                    iterate_recursively(item)

    iterate_recursively(compute)


def set_compute_suspicious_status(uuid, status):
    compute = db.get_root()['oms_root']['machines'][uuid]
    compute.suspicious = bool(status)

    def iterate_recursively(container):
        seen = set()
        for item in container.listcontent():
            if ICompute.providedBy(item):
                item.suspicious = bool(status)

                from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
                if (IVirtualizationContainer.providedBy(item) or ICompute.providedBy(item)):
                    if item.__name__ not in seen:
                        seen.add(item.__name__)
                        iterate_recursively(item)

    iterate_recursively(compute)


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
        yield self.gather_users()
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
            yield delete_machines(hosts_to_delete)

    @defer.inlineCallbacks
    def gather_users(self):
        # Automatically fills in Home with existing users
        @db.transact
        def get_users():
            home = db.get_root()['oms_root']['home']
            auth = getUtility(IAuthentication)
            for pname, pobj in auth.principals.iteritems():
                if type(pobj) is User:
                    if pobj.id not in home.listnames():
                        up = UserProfile(pobj.id, pobj.groups, uid=pobj.uid)
                        log.msg('Adding %s to /home' % (up))
                        home.add(up)
                    else:
                        if pobj.uid != home[pobj.id].uid:
                            home[pobj.id].uid = pobj.uid
                        if pobj.groups != home[pobj.id].groups:
                            home[pobj.id].groups = pobj.groups
        yield get_users()

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

    def delete_outstanding_request(self, compute):
        if str(compute) in self.outstanding_requests:
            del self.outstanding_requests[str(compute)]
        else:
            log.msg('Unlock: %s not in outstanding requests: %s'
                    % (str(compute), self.outstanding_requests.keys()), system='sync-unlock')

    @db.transact
    def handle_error(self, e, action, c, compute):
        e.trap(Exception)
        log.msg("Got exception on %s of '%s': %s" % (action, c, e), system='sync')
        if get_config().getboolean('debug', 'print_exceptions'):
            log.err(system='sync')
        self.delete_outstanding_request(compute)
        set_compute_failure_status(compute.__name__, True)

    @db.transact
    def handle_success(self, r, action, hostname, compute):
        log.msg("%s completed: '%s'" % (action, hostname), system='sync')
        self.delete_outstanding_request(compute)
        set_compute_failure_status(compute.__name__, False)

    @db.transact
    def handle_remote_error(self, ore, c, compute):
        ore.trap(OperationRemoteError)
        if ore.value.remote_tb and get_config().getboolean('debug', 'print_exceptions'):
            log.err(system='sync')
        else:
            log.msg(str(ore.value), system='sync', logLevel=ERROR)
        self.delete_outstanding_request(compute)
        set_compute_failure_status(compute.__name__, True)

    def execute_sync_action(self, hostname, compute):
        log.msg("Syncing started: '%s' (%s)" % (hostname, str(compute)), system='sync')
        syncaction = SyncAction(compute)
        deferred = syncaction.execute(DetachedProtocol(), object())
        deferred.addCallback(self.handle_success, 'sync action', hostname, compute)
        deferred.addErrback(self.handle_remote_error, hostname, compute)
        deferred.addErrback(self.handle_error, 'Sync action', hostname, compute)
        curtime = datetime.now().isoformat()
        self.outstanding_requests[str(compute)] = [deferred, curtime, 0, defer.Deferred()]
        return deferred

    @defer.inlineCallbacks
    def execute_ping_tests(self):
        for compute, hostname in (yield get_manageable_machines()):
            targetkey = str(compute)
            curtime = datetime.now().isoformat()

            if (targetkey in self.outstanding_requests and self.outstanding_requests[targetkey][2] > 5):
                log.msg('Killing all previous requests to %s (%s)' % (hostname, targetkey),
                        system='sync')
                self.outstanding_requests[targetkey][3].callback(None)
                del self.outstanding_requests[targetkey]

            if (targetkey not in self.outstanding_requests or self.outstanding_requests[targetkey][2] > 5):
                log.msg('Pinging %s (%s)...' % (hostname, compute), system='sync')
                pingtest = IPing(compute)
                killhook = defer.Deferred()
                deferred = pingtest.run(__killhook=killhook)
                self.outstanding_requests[targetkey] = [deferred, curtime, 0, killhook]
                deferred.addCallback(self.handle_success, 'ping test', hostname, compute)
                deferred.addErrback(self.handle_remote_error, hostname, compute)
                deferred.addErrback(self.handle_error, 'Ping test', hostname, compute)

                def sync_action(r, hostname, compute):
                    self.execute_sync_action(hostname, compute)

                deferred.addCallback(sync_action, hostname, compute)
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
