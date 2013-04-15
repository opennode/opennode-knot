import logging
import datetime
import time

from grokcore.component import Adapter, context
from twisted.internet import defer
from twisted.python import log
from zope.component import provideSubscriptionAdapter, queryAdapter
from zope.interface import implements, Interface

from opennode.knot.backend.operation import IGetGuestMetrics, IGetHostMetrics, OperationRemoteError
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.model.compute import IManageable, ICompute, IVirtualCompute
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
            computes = filter(lambda c: c and ICompute.providedBy(c) and not c.failure,
                         map(follow_symlinks, oms_root['computes'].listcontent()))
            gatherers = filter(None, (queryAdapter(c, IMetricsGatherer) for c in computes))
            return gatherers

        def handle_success(r, c):
            self.log_msg('%s: metrics gathered' % (c), logLevel=logging.DEBUG)
            if str(c) in self.outstanding_requests:
                del self.outstanding_requests[str(c)]

        def handle_errors(e, c):
            e.trap(Exception)
            self.log_msg("%s: got exception when gathering metrics: %s" % (c, e),
                         logLevel=logging.ERROR)
            self.log_err()
            if str(c) in self.outstanding_requests:
                del self.outstanding_requests[str(c)]

        for g in (yield get_gatherers()):
            hostname = yield db.get(g.context, 'hostname')
            targetkey = str(g.context)
            if (targetkey not in self.outstanding_requests
                or self.outstanding_requests[targetkey][2] > 5):

                if (targetkey in self.outstanding_requests and
                    self.outstanding_requests[targetkey][2] > 5):
                    self.log_msg('Killing all previous requests to %s (%s)' % (hostname, targetkey))
                    self.outstanding_requests[targetkey][3].kill()

                self.log_msg('%s: gathering metrics %s' % (hostname,
                                                           '(after timeout!)' if targetkey in
                                                           self.outstanding_requests else ''),
                                                           logLevel=logging.DEBUG)
                d = g.gather()
                curtime = datetime.datetime.now().isoformat()
                self.outstanding_requests[targetkey] = [d, curtime, 0, g]
                d.addCallback(handle_success, g.context)
                d.addErrback(handle_errors, g.context)
            else:
                self.outstanding_requests[targetkey][2] += 1
                self.log_msg('Skipping: another outstanding request to "%s" (%s) is found from %s.' %
                             (g.context, hostname, self.outstanding_requests[targetkey][1]),
                             logLevel=logging.DEBUG)


provideSubscriptionAdapter(subscription_factory(MetricsDaemonProcess), adapts=(Proc,))


class VirtualComputeMetricGatherer(Adapter):
    """Gathers VM metrics using IVirtualizationContainerSubmitter"""

    implements(IMetricsGatherer)
    context(IManageable)

    @defer.inlineCallbacks
    def gather(self):
        self._killhook = defer.Deferred()
        yield self.gather_vms()
        yield self.gather_phy()

    def kill(self):
        self._killhook.callback(None)

    @defer.inlineCallbacks
    def gather_vms(self):

        @db.ro_transact
        def get_vms_if_not_empty():
            vms = follow_symlinks(self.context['vms']) or []

            for vm in vms:
                if IVirtualCompute.providedBy(vm):
                    return vms

            log.msg('%s: no VMs' % (self.context.hostname), system='metrics', logLevel=logging.DEBUG)

        vms = yield get_vms_if_not_empty()

        # get the metrics for all running VMS
        if not vms or self.context.state != u'active':
            return

        name = yield db.get(self.context, 'hostname')

        try:
            log.msg('%s: gather VM metrics' % (name), system='metrics', logLevel=logging.DEBUG)
            submitter = IVirtualizationContainerSubmitter(vms)
            metrics = yield submitter.submit(IGetGuestMetrics, __killhook=self._killhook)
        except OperationRemoteError as e:
            log.msg('%s: remote error: %s' % (name, e), system='metrics', logLevel=logging.DEBUG)
            if e.remote_tb:
                log.msg(e.remote_tb, system='metrics', logLevel=logging.DEBUG)
            return
        except Exception:
            log.msg("%s: error gathering VM metrics" % name, system='metrics', logLevel=logging.ERROR)
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='metrics')

        if not metrics:
            log.msg('%s: no VM metrics received!' % name, system='metrics', logLevel=logging.WARNING)
            return

        log.msg('%s: VM metrics received: %s' % (name, len(metrics)), system='metrics')
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
        name = yield db.get(self.context, 'hostname')
        try:
            data = yield IGetHostMetrics(self.context).run(__killhook=self._killhook)

            log.msg('%s: host metrics received: %s' % (name, len(data)), system='metrics',
                    logLevel=logging.DEBUG)
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
            log.msg('%s: remote error: %s' % (name, e), system='metrics', logLevel=logging.WARNING)
        except Exception:
            log.msg("%s: error gathering host metrics" % name, system='metrics', logLevel=logging.ERROR)
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='metrics')
