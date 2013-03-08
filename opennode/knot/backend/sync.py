from logging import ERROR
from twisted.internet import defer
from twisted.python import log
from zope.component import provideSubscriptionAdapter, getAllUtilitiesRegisteredFor
from zope.interface import implements

from opennode.knot.backend.syncaction import SyncAction
from opennode.knot.backend.operation import OperationRemoteError
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
def get_machine_by_hostname(hostname):
    machines_by_name = db.get_root()['oms_root']['machines']['by-name']
    return follow_symlinks(machines_by_name[hostname])


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

    def handle_remote_error(self, ore, c):
        ore.trap(OperationRemoteError)
        if ore.value.remote_tb and get_config().getboolean('debug', 'print_exceptions'):
            log.err(system='sync')
        else:
            log.msg(str(ore.value), system='sync', logLevel=ERROR)
        del self.outstanding_requests[c]

    def handle_error(self, e, c):
        e.trap(Exception)
        log.msg("Got exception when syncing compute '%s': %s" % (c, e), system='sync')
        if get_config().getboolean('debug', 'print_exceptions'):
            log.err(system='sync')
        del self.outstanding_requests[c]

    def handle_success(self, r, c):
        log.msg("Syncing completed: '%s'" % c, system='sync')
        del self.outstanding_requests[c]

    @defer.inlineCallbacks
    def sync(self):
        log.msg('Synchronizing. Machines: %s' % (yield get_manageable_machine_hostnames()), system='sync')

        yield self.gather_machines()

        sync_actions = (yield self._getSyncActions())

        for c, deferred in sync_actions:
            deferred.addCallback(self.handle_success, c)
            deferred.addErrback(self.handle_remote_error, c)
            deferred.addErrback(self.handle_error, c)

    @defer.inlineCallbacks
    def _getSyncActions(self):
        sync_actions = []
        for i, hostname in (yield get_manageable_machines()):
            if hostname not in self.outstanding_requests:
                syncaction = SyncAction(i)
                log.msg("Syncing started: '%s'" % hostname, system='sync')
                deferred = syncaction.execute(DetachedProtocol(), object())
                self.outstanding_requests[hostname] = deferred
                sync_actions.append((hostname, deferred))
            else:
                log.msg("Syncing %s skipped: previous request not finished yet" % hostname, system='sync')

        defer.returnValue(sync_actions)

provideSubscriptionAdapter(subscription_factory(SyncDaemonProcess), adapts=(Proc,))
