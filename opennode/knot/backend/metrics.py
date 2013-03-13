import logging
import time

from grokcore.component import Adapter, context
from twisted.internet import defer
from twisted.python import log
from zope.component import provideSubscriptionAdapter, queryAdapter
from zope.interface import implements, Interface

from opennode.knot.backend.operation import IGetGuestMetrics, IGetHostMetrics, OperationRemoteError
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.model.compute import IManageable, ICompute
from opennode.oms.config import get_config
from opennode.oms.model.model.proc import IProcess, Proc, DaemonProcess
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.model.model.stream import IStream
from opennode.oms.util import subscription_factory, async_sleep
from opennode.oms.zodb import db


class IMetricsGatherer(Interface):
    def gather():
        """Gathers metrics for some object"""


class MetricsDaemonProcess(DaemonProcess):
    implements(IProcess)

    __name__ = "metrics"

    def __init__(self):
        super(MetricsDaemonProcess, self).__init__()
        self.interval = get_config().getint('metrics', 'interval')
        self.outstanding_requests = {}

    @defer.inlineCallbacks
    def run(self):
        while True:
            try:
                # Currently we have special codes for gathering info about machines
                # hostinginv VM, in future here we'll traverse the whole zodb and search for gatherers
                # and maintain the gatherers via add/remove events.
                if not self.paused:
                    yield self.gather_machines()
            except Exception:
                self.log_err()

            yield async_sleep(self.interval)

    def log_msg(self, msg, **kwargs):
        log.msg(msg, system='metrics', **kwargs)

    def log_err(self, msg=None, **kwargs):
        log.err(msg, system='metrics', **kwargs)

    @defer.inlineCallbacks
    def gather_machines(self):
        @db.ro_transact
        def get_gatherers():
            oms_root = db.get_root()['oms_root']
            computes = filter(lambda c: ICompute.providedBy(c) and not c.failure,
                         map(follow_symlinks, oms_root['computes'].listcontent()))

            gatherers = filter(None, (queryAdapter(c, IMetricsGatherer) for c in computes))
            return gatherers

        def handle_success(r, c):
            self.log_msg('Metrics gathered for %s' % (c), logLevel=logging.DEBUG)
            del self.outstanding_requests[c]

        def handle_errors(e, c):
            e.trap(Exception)
            self.log_msg("Got exception when gathering metrics compute '%s': %s" % (c, e))
            self.log_err()
            del self.outstanding_requests[c]

        for g in (yield get_gatherers()):
            hostname = yield db.get(g.context, 'hostname')
            if hostname not in self.outstanding_requests:
                self.log_msg('Gathering metrics for %s' % hostname, logLevel=logging.DEBUG)
                d = g.gather()
                self.outstanding_requests[hostname] = d
                d.addCallback(handle_success, hostname)
                d.addErrback(handle_errors, hostname)
            else:
                self.log_msg('Skipping: another outstanding request to "%s" is found.' % (hostname),
                             logLevel=logging.DEBUG)


provideSubscriptionAdapter(subscription_factory(MetricsDaemonProcess), adapts=(Proc,))


class VirtualComputeMetricGatherer(Adapter):
    """Gathers VM metrics using IVirtualizationContainerSubmitter"""

    implements(IMetricsGatherer)
    context(IManageable)

    @defer.inlineCallbacks
    def gather(self):
        yield self.gather_vms()
        yield self.gather_phy()

    @defer.inlineCallbacks
    def gather_vms(self):

        @db.ro_transact
        def get_vms():
            return follow_symlinks(self.context['vms'])
        vms = yield get_vms()

        name = yield db.get(self.context, 'hostname')

        # get the metrics for all running VMS
        if not vms or self.context.state != u'active':
            return

        try:
            metrics = yield IVirtualizationContainerSubmitter(vms).submit(IGetGuestMetrics)
        except OperationRemoteError as e:
            log.msg('Remote error: %s' % e, system='metrics-vm', logLevel=logging.DEBUG)
            if e.remote_tb:
                log.msg(e.remote_tb, system='metrics-vm')
            return

        if not metrics:
            log.msg('No vm metrics received for %s' % name, system='metrics-vm')
            return

        log.msg('VM metrics received for %s: %s' % (name, len(metrics)), system='metrics-vm')
        timestamp = int(time.time() * 1000)

        # db transact is needed only to traverse the zodb.
        @db.ro_transact
        def get_streams():
            streams = []
            for uuid, data in metrics.items():
                if vms[uuid] and vms[uuid]['metrics']:
                    vm_metrics = vms[uuid]['metrics']
                    for k in data:
                        if vm_metrics[k]:
                            streams.append((IStream(vm_metrics[k]), (timestamp, data[k])))
            return streams

        # streams could defer the data appending but we don't care
        for stream, data_point in (yield get_streams()):
            stream.add(data_point)

    @defer.inlineCallbacks
    def gather_phy(self):
        try:
            data = yield IGetHostMetrics(self.context).run()
            name = yield db.get(self.context, 'hostname')

            log.msg('Got data for metrics of %s: %s' % (name, len(data)), system='metrics-phy')
            timestamp = int(time.time() * 1000)

            # db transact is needed only to traverse the zodb.
            @db.ro_transact
            def get_streams():
                streams = []
                host_metrics = self.context['metrics']
                if host_metrics:
                    for k in data:
                        if host_metrics[k]:
                            streams.append((IStream(host_metrics[k]), (timestamp, data[k])))
                return streams

            for stream, data_point in (yield get_streams()):
                stream.add(data_point)
        except OperationRemoteError as e:
            log.msg(e, system='metrics-phy')
        except Exception:
            log.msg("Error gathering phy metrics", system='metrics-phy')
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='metrics-phy')
