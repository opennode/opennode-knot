from __future__ import absolute_import

from salt.client import LocalClient
from grokcore.component import Adapter, context, baseclass
from twisted.internet import defer, threads, reactor
from zope.interface import classImplements
import multiprocessing
import cPickle
import time

from opennode.knot.backend.operation import (IGetComputeInfo, IStartVM, IShutdownVM, IDestroyVM, ISuspendVM,
                                             IResumeVM, IRebootVM, IListVMS, IHostInterfaces, IDeployVM,
                                             IUndeployVM, IGetGuestMetrics, IGetHostMetrics,
                                             IGetLocalTemplates, IMinion, IGetSignedCertificateNames,
                                             IGetVirtualizationContainers, IGetDiskUsage, IGetRoutes,
                                             IGetIncomingHosts, ICleanupHost,
                                             IAcceptIncomingHost, IGetHWUptime)
from opennode.knot.model.compute import ISaltInstalled
from opennode.knot.utils.logging import log
from opennode.oms.config import get_config
from opennode.oms.model.model.proc import Proc
from opennode.oms.security.principals import effective_principals
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
        client = LocalClient(
                c_path=get_config().get('salt', 'master_config_path', '/etc/salt/master'))

        log('running action: %s args: %s' %(self.action, self.args), 'salt')
        data = client.cmd(self.hostname, self.action, arg=self.args)
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

    def register_salt_proc(self, args, **kwargs):
        Proc.register(self.deferred,
                      "/bin/salt '%s' %s %s" % (self.hostname.encode('utf-8'), self.action,
                                                ' '.join(str(i) for i in args)), **kwargs)


class AsyncSaltExecutor(SaltExecutor):
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

            try:
                client.provide_args(self.hostname, self.action, args)
                client.start()
                self.register_salt_proc(args)
            except SystemExit:
                log('failed action: %s on host: %s' % (self.action, self.hostname), 'salt')
            self.start_polling()

        spawn()
        return self.deferred

    @db.ro_transact
    def start_polling(self):
        status = self._get_client().is_alive()

        if status:
            pdata = self._get_client().q.get()
            results = cPickle.loads(pdata)
            self._fire_events(results)
            return
        reactor.callLater(self.interval, self.start_polling)

    def _fire_events(self, data):
        # noglobs=True and async=True cannot live together
        # see http://goo.gl/UgrZu
        # thus we need a robust way to get the result for this host,
        # even when the host names don't match (e.g. localhost vs real host name).
        hostkey = self.hostname
        if len(data.keys()) == 1:
            hostkey = data.keys()[0]

        res = data[hostkey]

        if res and isinstance(res, list) and res[0] == 'REMOTE_ERROR':
            self.deferred.errback(Exception(*res[1:]))
        else:
            self.deferred.callback(res)


class SyncSaltExecutor(SaltExecutor):

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

        if until > now :
            raise Exception("Host %s was temporarily blacklisted. %s s to go" % (self.hostname, until - now))

        if self.hostname in self.host_blacklist:
            print "[salt] removing %s from blacklist" % self.hostname
            del self.host_blacklist[self.hostname]

        @timeout(hard_timeout)
        def spawn():
            return threads.deferToThread(spawn_real)

        def spawn_real():
            client = self._get_client()
            try:
                client.provide_args(self.hostname, self.action, args)
                client.start()
                client.join()
                pdata = client.q.get()
                data = cPickle.loads(pdata)
            except SystemExit:
                log('failed action: %s on host: %s' % (self.action, self.hostname), 'salt')
            else:
                hostkey = self.hostname
                if len(data.keys()) == 1:
                    hostkey = data.keys()[0]
                res = data[hostkey]
                if res and isinstance(res, list) and res[0] == 'REMOTE_ERROR':
                    raise Exception(*res[1:])

                return res

        @defer.inlineCallbacks
        def spawn_handle_timeout():
            try:
                res = yield spawn()
                defer.returnValue(res)
            except TimeoutException as e:
                print "[salt] Got timeout while executing %s on %s (%s)" % (self.action, self.hostname, e)
                if blacklist_enabled:
                    if self.hostname not in whitelist:
                        print "[salt] blacklisting %s for %s s" % (self.hostname, blacklist_ttl)
                        self.host_blacklist[self.hostname] = time.time()
                    else:
                        print "[salt] host %s not blacklisted because in 'timeout_whitelist'" % self.hostname
                raise

        self.deferred = spawn_handle_timeout()

        if self.interaction:
            principals = effective_principals(self.interaction)
            if principals:
                principal = principals[0]
                self.register_salt_proc(args, principal=principal)

        return self.deferred

class SaltBase(Adapter):
    """Base class for all Salt method calls."""
    context(ISaltInstalled)
    baseclass()

    action = None
    __executor__ = None

    executor_classes = {'sync': SyncSaltExecutor,
                        'async': AsyncSaltExecutor,}

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
    }

OVERRIDE_EXECUTORS = {
    IDeployVM: AsyncSaltExecutor,
    IUndeployVM: AsyncSaltExecutor
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
