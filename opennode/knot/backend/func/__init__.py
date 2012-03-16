from __future__ import absolute_import

import time

from func import jobthing
from func.overlord.client import Overlord
from grokcore.component import Adapter, context, baseclass
from twisted.internet import defer, reactor, threads
from zope.interface import classImplements

from opennode.knot.backend.operation import (IFuncInstalled, IGetComputeInfo, IStartVM, IShutdownVM, IDestroyVM,
                                            ISuspendVM, IResumeVM, IRebootVM, IListVMS, IHostInterfaces, IDeployVM,
                                            IUndeployVM, IGetGuestMetrics, IGetHostMetrics, IGetLocalTemplates,
                                            IFuncMinion, IGetSignedCertificateNames, IGetVirtualizationContainers,
                                            IGetDiskUsage, IGetRoutes, IGetIncomingHosts, ICleanupHost,
                                            IAcceptIncomingHost, IGetHWUptime)
from opennode.oms.config import get_config
from opennode.oms.model.model.proc import Proc
from opennode.oms.util import timeout, TimeoutException
from opennode.oms.zodb import db


class FuncExecutor(object):
    pass


class AsyncFuncExecutor(FuncExecutor):
    interval = 0.1

    def __init__(self, hostname, func_action):
        self.hostname = hostname
        self.func_action = func_action

    def run(self, *args, **kwargs):
        self.deferred = defer.Deferred()

        @db.ro_transact
        def spawn_func():
            client = self._get_client()
            # we assume that all of the func actions are in the form of 'module.action'
            module_action, action_name = self.func_action.rsplit('.', 1)
            module = getattr(client, module_action)
            action = getattr(module, action_name)

            self.job_id = action(*args, **kwargs)

            Proc.register(self.deferred, "/bin/func '%s' call %s %s" % (self.hostname.encode('utf-8'),
                                                                        self.func_action,
                                                                        ' '.join(str(i) for i in args)))

            self.start_polling()

        spawn_func()
        return self.deferred

    @db.ro_transact
    def start_polling(self):
        return_code, results = self._get_client().job_status(self.job_id)

        if return_code in (jobthing.JOB_ID_FINISHED, jobthing.JOB_ID_REMOTE_ERROR):
            self._fire_events(results)
            return
        if return_code == jobthing.JOB_ID_LOST_IN_SPACE:
            self.deferred.errback(Exception('Command lost in space'))
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

    overlords = {}

    @db.assert_transact
    def _get_client(self):
        """Returns an instance of the Overlord."""
        if self.hostname not in self.overlords:
            self.overlords[self.hostname] = Overlord(self.hostname, async=True)
        return self.overlords[self.hostname]


class SyncFuncExecutor(FuncExecutor):

    # Contains a blacklist of host which had strange problems with func
    # so we temporarily avoid calling them again until the blacklist TTL expires
    func_host_blacklist = {}

    def __init__(self, hostname, func_action):
        self.hostname = hostname
        self.func_action = func_action

    def run(self, *args, **kwargs):
        hard_timeout = get_config().getint('func', 'hard_timeout')
        blacklist_enabled = get_config().getboolean('func', 'timeout_blacklist')
        blacklist_ttl = get_config().getint('func', 'timeout_blacklist_ttl')
        whitelist = [i.strip() for i in get_config().get('func', 'timeout_whitelist').split(',')]

        now = time.time()
        until = self.func_host_blacklist.get(self.hostname, 0) + blacklist_ttl
        if until > now :
            raise Exception("Host %s was temporarily blacklisted. %s s to go" % (self.hostname, until - now))
        if self.hostname in self.func_host_blacklist:
            print "[func] removing %s from blacklist" % self.hostname
            del self.func_host_blacklist[self.hostname]

        @timeout(hard_timeout)
        def spawn_func():
            return threads.deferToThread(spawn_func_real)

        def spawn_func_real():
            client = self._get_client()
            # we assume that all of the func actions are in the form of 'module.action'
            module_action, action_name = self.func_action.rsplit('.', 1)
            module = getattr(client, module_action)
            action = getattr(module, action_name)

            data = action(*args, **kwargs)
            # noglobs=True and async=True cannot live together
            # see http://goo.gl/UgrZu
            # thus we need a robust way to get the result for this host,
            # even when the host names don't match (e.g. localhost vs real host name).
            hostkey = self.hostname
            if len(data.keys()) == 1:
                hostkey = data.keys()[0]
            res = data[hostkey]
            if res and isinstance(res, list) and res[0] == 'REMOTE_ERROR':
                raise Exception(*res[1:])
            else:
                return res

        @defer.inlineCallbacks
        def spawn_func_handle_timeout():
            try:
                res = yield spawn_func()
                defer.returnValue(res)
            except TimeoutException as e:
                print "[func] Got timeout while executing %s on %s (%s)" % (self.func_action, self.hostname, e)
                if blacklist_enabled:
                    if self.hostname not in whitelist:
                        print "[func] blacklisting %s for %s s" % (self.hostname, blacklist_ttl)
                        self.func_host_blacklist[self.hostname] = time.time()
                    else:
                        print "[func] host %s not blacklisted because in 'timeout_whitelist'" % self.hostname
                raise

        self.deferred = spawn_func_handle_timeout()
        # XXX: after switching to the 'synchronous' func execute, this is broken, TODO check why!
        # Proc.register(self.deferred, "/bin/func '%s' call %s %s" % (self.hostname.encode('utf-8'),
        #                                                             self.func_action,
        #                                                             ' '.join(str(i) for i in args)))

        return self.deferred

    overlords = {}

    @db.assert_transact
    def _get_client(self):
        """Returns an instance of the Overlord."""
        if self.hostname not in self.overlords:
            self.overlords[self.hostname] = Overlord(self.hostname, async=False)
        return self.overlords[self.hostname]


class FuncBase(Adapter):
    """Base class for all Func method calls."""
    context(IFuncInstalled)
    baseclass()

    func_action = None

    executor_classes = {'sync': SyncFuncExecutor,
                        'async': AsyncFuncExecutor,
                        }

    def run(self, *args, **kwargs):
        executor_class = self.executor_classes[get_config().get('func', 'executor_class')]
        executor = executor_class(IFuncMinion(self.context).hostname(), self.func_action)
        return executor.run(*args, **kwargs)


FUNC_ACTIONS = {IGetComputeInfo: 'hardware.info', IStartVM: 'onode.vm.start_vm',
                IShutdownVM: 'onode.vm.shutdown_vm', IDestroyVM: 'onode.vm.destroy_vm',
                ISuspendVM: 'onode.vm.suspend_vm', IResumeVM: 'onode.vm.resume_vm',
                IRebootVM: 'onode.vm.reboot_vm', IListVMS: 'onode.vm.list_vms',
                IDeployVM: 'onode.vm.deploy_vm', IUndeployVM: 'onode.vm.undeploy_vm',
                IGetVirtualizationContainers: 'onode.vm.autodetected_backends',
                IGetGuestMetrics: 'onode.vm.metrics', IGetHostMetrics: 'onode.metrics',
                IGetLocalTemplates: 'onode.vm.get_local_templates',
                IGetSignedCertificateNames: 'certmastermod.get_signed_certs',
                IGetIncomingHosts: 'certmastermod.get_hosts_to_sign',
                ICleanupHost: 'certmastermod.cleanup_hosts',
                IAcceptIncomingHost: 'certmastermod.sign_hosts',
                IGetDiskUsage: 'disk.usage', IGetRoutes: 'onode.network.show_routing_table',
                IHostInterfaces: 'onode.host.interfaces',
                IGetHWUptime: 'onode.host.uptime'}


# Avoid polluting the global namespace with temporary variables:
def _generate_classes():
    # Dynamically generate an adapter class for each supported Func function:
    for interface, action in FUNC_ACTIONS.items():
        cls_name = 'Func%s' % interface.__name__[1:]
        cls = type(cls_name, (FuncBase, ), dict(func_action=action))
        classImplements(cls, interface)
        globals()[cls_name] = cls
_generate_classes()
