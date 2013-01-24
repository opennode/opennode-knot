from grokcore.component import Adapter, context
from twisted.internet import defer
from twisted.python import log
from zope.component import provideSubscriptionAdapter, queryAdapter
from zope.interface import implements, Interface
import time

from opennode.knot.backend.operation import IGetGuestMetrics, IGetHostMetrics, OperationRemoteError
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.model.compute import IManageable
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

    @defer.inlineCallbacks
    def run(self):
        while True:
            try:
                # Currently we have special codes for gathering info about machines
                # hostinginv VM, in future here we'll traverse the whole zodb and search for gatherers
                # and maintain the gatherers via add/remove events.
                if not self.paused:
                    yield self.gather_machines()
            except Exception :
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
            res = []

            oms_root = db.get_root()['oms_root']
            res = filter(None, (queryAdapter(follow_symlinks(i), IMetricsGatherer)
                                        for i in oms_root['computes'].listcontent()))
            return res

        for i in (yield get_gatherers()):
            try:
                yield i.gather()
            except Exception as e:
                self.log_msg("Got exception when gathering metrics compute '%s': %s" % (i.context, e))
                self.log_err()


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
            return self.context['vms-openvz']
        vms = yield get_vms()

        # get the metrics for all running VMS
        if not vms or self.context.state != u'active':
            return

        metrics = yield IVirtualizationContainerSubmitter(vms).submit(IGetGuestMetrics)

        if not metrics:
            log.msg('No vm metrics received', system='metrics')
            return

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
            log.msg(e, system='metrics')
        except Exception:
            log.msg("Error gathering phy metrics", system='metrics')
            if get_config().getboolean('debug', 'print_exceptions'):
                log.err(system='metrics')
