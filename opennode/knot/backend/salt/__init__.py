from __future__ import absolute_import

from grokcore.component import Adapter, context, baseclass
from salt.client import LocalClient
from twisted.internet import defer, threads, reactor
from twisted.python import log
from zope.interface import classImplements
import cPickle
import logging
import multiprocessing
import time

from opennode.knot.backend.operation import (IGetComputeInfo, IStartVM, IShutdownVM, IDestroyVM, ISuspendVM,
                                             IResumeVM, IRebootVM, IListVMS, IHostInterfaces, IDeployVM,
                                             IUndeployVM, IGetGuestMetrics, IGetHostMetrics,
                                             IGetLocalTemplates, IMinion, IGetSignedCertificateNames,
                                             IGetVirtualizationContainers, IGetDiskUsage, IGetRoutes,
                                             IGetIncomingHosts, ICleanupHost, IMigrateVM,
                                             IAcceptIncomingHost, IGetHWUptime, OperationRemoteError)
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.config import get_config
from opennode.oms.util import timeout, TimeoutException
from opennode.oms.zodb import db


class SaltMultiprocessingClient(multiprocessing.Process):

    def __init__(self):
        self.q = multiprocessing.Queue()
        super(SaltMultiprocessingClient, self).__init__()

    def provide_args(self, hostname, action, args):
        self.hostname = hostname
        self.action = action
        self.args = args

    def run(self):
        client = LocalClient(c_path=get_config().get('salt', 'master_config_path', '/etc/salt/master'))
        try:
            log.msg('running action: %s args: %s' % (self.action, self.args), system='salt',
                    logLevel=logging.DEBUG)
            data = client.cmd(self.hostname, self.action, arg=self.args)
        except SystemExit as e:
            log.err('failed action: %s on host: %s (%s)' % (self.action, self.hostname, e), system='salt')
            self.q.put(cPickle.dumps({}))
        else:
            pdata = cPickle.dumps(data)
            self.q.put(pdata)


class SaltExecutor(object):
    client = None

    @db.assert_transact
    def _get_client(self):
        """Returns an instance of Salt Stack LocalClient."""
        if not self.client:
            self.client = SaltMultiprocessingClient()
        return self.client

    def _destroy_client(self):
        self.client = None


class AsynchronousSaltExecutor(SaltExecutor):
    interval = 0.1

    def __init__(self, hostname, action, interaction):
        self.hostname = hostname
        self.action = action
        self.interaction = interaction

    def run(self, *args, **kwargs):
        self.deferred = defer.Deferred()

        @db.ro_transact
        def spawn():
            client = self._get_client()
            client.provide_args(self.hostname, self.action, args)
            client.start()
            self.poll()

        spawn()
        return self.deferred

    @db.ro_transact
    def poll(self):
        done = not self._get_client().is_alive()

        if done:
            pdata = self._get_client().q.get()
            results = cPickle.loads(pdata)
            self._fire_events(results)
            return

        reactor.callLater(self.interval, self.poll)

    def _fire_events(self, data):
        hostkey = self.hostname if len(data.keys()) != 1 else data.keys()[0]

        if hostkey not in data:
            self.deferred.errback(OperationRemoteError(msg='Remote returned empty response'))
        elif type(data[hostkey]) is str and data[hostkey].startswith('Traceback'):
            self.deferred.errback(OperationRemoteError(msg="Remote error on %s" % hostkey,
                                                       remote_tb=data[hostkey]))
        else:
            self.deferred.callback(data[hostkey])


class SynchronousSaltExecutor(SaltExecutor):

    # Contains a blacklist of host which had strange problems with salt
    # so we temporarily avoid calling them again until the blacklist TTL expires
    host_blacklist = {}

    def __init__(self, hostname, action, interaction):
        self.hostname = hostname
        self.action = action
        self.interaction = interaction

    def run(self, *args, **kwargs):
        hard_timeout = get_config().getint('salt', 'hard_timeout')
        blacklist_enabled = get_config().getboolean('salt', 'timeout_blacklist')
        blacklist_ttl = get_config().getint('salt', 'timeout_blacklist_ttl')
        whitelist = [i.strip() for i in get_config().get('salt', 'timeout_whitelist').split(',')]

        now = time.time()
        until = self.host_blacklist.get(self.hostname, 0) + blacklist_ttl

        if until > now:
            raise Exception("Host %s was temporarily blacklisted. %s s to go" % (self.hostname, until - now))

        if self.hostname in self.host_blacklist:
            log.msg("removing %s from blacklist" % self.hostname, system='salt', logLevel=logging.DEBUG)
            del self.host_blacklist[self.hostname]

        @timeout(hard_timeout)
        def spawn():
            return threads.deferToThread(spawn_real)

        def spawn_real():
            client = self._get_client()
            client.provide_args(self.hostname, self.action, args)
            client.start()
            client.join()
            pdata = client.q.get()
            data = cPickle.loads(pdata)
            hostkey = self.hostname

            if len(data.keys()) == 1:
                hostkey = data.keys()[0]

            if hostkey not in data:
                raise OperationRemoteError(
                    msg='Response for %s on \'%s%s\' was empty' % (hostkey, self.action, args))

            if type(data[hostkey]) is str and data[hostkey].startswith('Traceback'):
                raise OperationRemoteError(msg='Remote error on %s' % (hostkey), remote_tb=data[hostkey])

            return data[hostkey]

        @defer.inlineCallbacks
        def spawn_handle_timeout():
            try:
                res = yield spawn()
                defer.returnValue(res)
            except TimeoutException as e:
                client = self._get_client()
                client.terminate()
                self._destroy_client()
                log.msg("Got timeout while executing %s on %s (%s)" % (self.action, self.hostname, e),
                        system='salt')
                if blacklist_enabled:
                    if self.hostname not in whitelist:
                        log.msg("blacklisting %s for %s s" % (self.hostname, blacklist_ttl),
                                system='salt')
                        self.host_blacklist[self.hostname] = time.time()
                    else:
                        log.msg("host %s not blacklisted because in 'timeout_whitelist'" % self.hostname,
                                system='salt')
                raise

        self.deferred = spawn_handle_timeout()

        return self.deferred


class SaltBase(Adapter):
    """Base class for all Salt method calls."""
    context(ISaltInstalled)
    baseclass()

    action = None
    __executor__ = None

    executor_classes = {'sync': SynchronousSaltExecutor,
                        'async': AsynchronousSaltExecutor}

    @defer.inlineCallbacks
    def run(self, *args, **kwargs):
        executor_class = self.__executor__
        hostname = yield IMinion(self.context).hostname()
        interaction = db.context(self.context).get('interaction', None)
        executor = executor_class(hostname, self.action, interaction)
        res = yield executor.run(*args, **kwargs)
        defer.returnValue(res)


ACTIONS = {
    IAcceptIncomingHost: 'saltmod.sign_hosts',
    ICleanupHost: 'saltmod.cleanup_hosts',
    IDeployVM: 'onode.vm_deploy_vm',
    IDestroyVM: 'onode.vm_destroy_vm',
    IGetComputeInfo: 'onode.hardware_info',
    IGetDiskUsage: 'onode.host_disk_usage',
    IGetGuestMetrics: 'onode.vm_metrics',
    IGetHWUptime: 'onode.host_uptime',
    IGetHostMetrics: 'onode.host_metrics',
    IGetIncomingHosts: 'saltmod.get_hosts_to_sign',
    IGetLocalTemplates: 'onode.vm_get_local_templates',
    IGetRoutes: 'onode.network_show_routing_table',
    IGetSignedCertificateNames: 'saltmod.get_signed_certs',
    IGetVirtualizationContainers: 'onode.vm_autodetected_backends',
    IHostInterfaces: 'onode.host_interfaces',
    IListVMS: 'onode.vm_list_vms',
    IRebootVM: 'onode.vm_reboot_vm',
    IResumeVM: 'onode.vm_resume_vm',
    IShutdownVM: 'onode.vm_shutdown_vm',
    IStartVM: 'onode.vm_start_vm',
    ISuspendVM: 'onode.vm_suspend_vm',
    IUndeployVM: 'onode.vm_undeploy_vm',
    IMigrateVM: 'onode.vm_migrate'
}

OVERRIDE_EXECUTORS = {
    IDeployVM: AsynchronousSaltExecutor,
    IUndeployVM: AsynchronousSaltExecutor
}


# Avoid polluting the global namespace with temporary variables:
def _generate_classes():
    # Dynamically generate an adapter class for each supported Salt salttion:
    for interface, action in ACTIONS.items():
        cls_name = 'Salt%s' % interface.__name__[1:]
        cls = type(cls_name, (SaltBase, ), dict(action=action))
        classImplements(cls, interface)
        executor = get_config().get('salt', 'executor_class')
        cls.__executor__ = OVERRIDE_EXECUTORS.get(interface, SaltBase.executor_classes[executor])
        globals()[cls_name] = cls
_generate_classes()
