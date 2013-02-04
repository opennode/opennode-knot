from __future__ import absolute_import

from grokcore.component import Adapter, context, baseclass
from twisted.internet import defer, threads, reactor
from twisted.python import log
from zope.interface import classImplements
import cPickle
import logging
import multiprocessing
import Queue
import subprocess
import time

from opennode.knot.backend import operation as op
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
        try:
            from salt.client import LocalClient
            client = LocalClient(c_path=get_config().getstring('salt', 'master_config_path',
                                                               '/etc/salt/master'))
            try:
                log.msg('Running action against "%s": %s args: %s' % (self.hostname, self.action, self.args),
                        system='salt-local', logLevel=logging.DEBUG)
                data = client.cmd(self.hostname, self.action, arg=self.args)
            except SystemExit as e:
                log.msg('Failed action %s on host: %s (%s)' % (self.action, self.hostname, e),
                        system='salt-local')
                self.q.put(cPickle.dumps({}))
            else:
                pdata = cPickle.dumps(data)
                self.q.put(pdata)
        except Exception:
            log.err(system='salt-local')


class SaltRemoteClient(multiprocessing.Process):

    def __init__(self):
        self.q = Queue.Queue()
        super(SaltRemoteClient, self).__init__()

    def provide_args(self, hostname, action, args):
        self.hostname = hostname
        self.action = action
        self.args = args

    def run(self):
        try:
            try:
                log.msg('Running action against "%s": %s args: %s' % (self.hostname, self.action, self.args),
                        system='salt-remote', logLevel=logging.DEBUG)
                cmd = get_config().getstring('salt', 'remote_command', 'salt')
                # XXX: instead of raw+eval (which is dangerous) we could use json or yaml
                output = subprocess.check_output(cmd.split(' ') +
                                                 ['--no-color', '--out=raw', self.hostname, self.action] +
                                                 map(str, self.args))
                if output:
                    data = eval(output)
                else:
                    data = {}
            except subprocess.CalledProcessError as e:
                log.msg('Failed action %s on host: %s (%s)' % (self.action, self.hostname, e),
                        system='salt-remote')
                self.q.put(cPickle.dumps({}))
            else:
                pdata = cPickle.dumps(data)
                self.q.put(pdata)
        except Exception:
            log.err(system='salt-remote')


class SaltExecutor(object):
    client = None

    def _get_client(self):
        """Returns an instance of Salt Stack LocalClient."""
        if not self.client:
            if get_config().getstring('salt', 'remote_command', None):
                self.client = SaltRemoteClient()
            else:
                self.client = SaltMultiprocessingClient()
        return self.client

    def _destroy_client(self):
        if self.client is not None:
            self.client.terminate()
            self.client = None


class AsynchronousSaltExecutor(SaltExecutor):
    interval = 0.1

    def __init__(self, hostname, action, interaction):
        self.hostname = hostname
        self.action = action
        self.interaction = interaction

    def run(self, *args, **kwargs):
        self.deferred = defer.Deferred()

        client = self._get_client()
        client.provide_args(self.hostname, self.action, args)
        client.start()

        self.poll()

        return self.deferred

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
            self.deferred.errback(op.OperationRemoteError(msg='Remote returned empty response'))
        elif type(data[hostkey]) is str and data[hostkey].startswith('Traceback'):
            self.deferred.errback(op.OperationRemoteError(msg="Remote error on %s" % hostkey,
                                                          remote_tb=data[hostkey]))
        elif type(data[hostkey]) is str and data[hostkey].endswith('is not available.'):
            # TODO: mark the host as unmanageable (agent modules are missing)
            self.deferred.errback(op.OperationRemoteError(msg="Remote error on %s: module unavailable" %
                                                          hostkey))
        else:
            self.deferred.callback(data[hostkey])


class SynchronousSaltExecutor(SaltExecutor):

    # Contains a blacklist of host which had strange problems with salt
    # so we temporarily avoid calling them again until the blacklist TTL expires
    host_blacklist = {}
    stall_countermeasure_applied = None

    def __init__(self, hostname, action, interaction):
        self.hostname = hostname
        self.action = action
        self.interaction = interaction

    def run(self, *args, **kwargs):
        hard_timeout = get_config().getint('salt', 'hard_timeout')
        blacklist_ttl = get_config().getint('salt', 'timeout_blacklist_ttl')

        now = time.time()
        until = self.host_blacklist.get(self.hostname, 0) + blacklist_ttl

        if until > now:
            raise Exception("Host %s was temporarily blacklisted. %s s to go" % (self.hostname, until - now))

        if self.hostname in self.host_blacklist:
            log.msg("Removing '%s' from blacklist" % self.hostname, system='salt', logLevel=logging.DEBUG)
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
                raise op.OperationRemoteError(
                    msg='Response for %s on \'%s%s\' was empty' % (hostkey, self.action, args))

            hdata = data[hostkey]

            if type(hdata) is str and hdata.startswith('Traceback'):
                raise op.OperationRemoteError(msg='Remote error on %s' % (hostkey), remote_tb=data[hostkey])

            if type(hdata) is str and hdata.endswith('is not available.'):
                # TODO: mark the host as unmanageable (agent modules are missing)
                raise op.OperationRemoteError(msg="Remote error on %s: module unavailable" % hostkey)

            return hdata

        @defer.inlineCallbacks
        def spawn_handle_timeout():
            try:
                res = yield spawn()
                defer.returnValue(res)
            except TimeoutException as e:
                self._destroy_client()
                log.msg("Timeout while executing '%s' on '%s' (%s)" % (self.action, self.hostname, e),
                        system='salt')
                self.blacklist(blacklist_ttl)
                res = yield self.launch_stall_countermeasure(*args, **kwargs)
                defer.returnValue(res)

        self.deferred = spawn_handle_timeout()

        return self.deferred

    def blacklist(self, blacklist_ttl):
        blacklist_enabled = get_config().getboolean('salt', 'timeout_blacklist')
        whitelist = [i.strip() for i in get_config().getstring('salt', 'timeout_whitelist').split(',')]
        if blacklist_enabled:
            if self.hostname not in whitelist:
                log.msg("blacklisting %s for %s s" % (self.hostname, blacklist_ttl), system='salt')
                self.host_blacklist[self.hostname] = time.time()
            else:
                log.msg("host %s is whitelisted" % self.hostname, system='salt')

    @defer.inlineCallbacks
    def launch_stall_countermeasure(self, *args, **kwargs):
        if self.stall_countermeasure_applied is not None:
            now = time.time()
            ttl = get_config().getint('salt', 'timeout_blacklist_ttl', 600)
            if now > self.stall_countermeasure_applied + ttl:
                log.msg('Last stall countermeasure has been applied %s seconds ago. Reapplying.' %
                        (now - self.stall_countermeasure_applied), system='salt')
            else:
                log.msg('Salt stall countermeasure seems ineffective, %s is down maybe?' % (self.hostname),
                        system='salt')
                raise TimeoutException()
        try:
            import subprocess
            self.stall_countermeasure_applied = time.time()
            # TODO: provide a hint for the hook by passing in salt/remote_command value
            script = get_config().getstring('salt', 'salt_countermeasure_script',
                                            './scripts/hooks/salt_stall')
            subprocess.check_call(script.split(' '))
            log.msg('Stall countermeasure applied. Retrying...', system='salt')
            defer.returnValue((yield self.run(*args, **kwargs)))
        except Exception:
            log.err(system='salt-stall')


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
        hostname = yield op.IMinion(self.context).hostname()
        interaction = db.context(self.context).get('interaction', None)
        executor = executor_class(hostname, self.action, interaction)
        res = yield executor.run(*args, **kwargs)
        defer.returnValue(res)


ACTIONS = {
    op.IAcceptIncomingHost: 'saltmod.sign_hosts',
    op.ICleanupHost: 'saltmod.cleanup_hosts',
    op.IDeployVM: 'onode.vm_deploy_vm',
    op.IDestroyVM: 'onode.vm_destroy_vm',
    op.IGetComputeInfo: 'onode.hardware_info',
    op.IGetDiskUsage: 'onode.host_disk_usage',
    op.IGetGuestMetrics: 'onode.vm_metrics',
    op.IGetHWUptime: 'onode.host_uptime',
    op.IGetHostMetrics: 'onode.host_metrics',
    op.IGetIncomingHosts: 'saltmod.get_hosts_to_sign',
    op.IGetLocalTemplates: 'onode.vm_get_local_templates',
    op.IGetRoutes: 'onode.network_show_routing_table',
    op.IGetSignedCertificateNames: 'saltmod.get_signed_certs',
    op.IGetVirtualizationContainers: 'onode.vm_autodetected_backends',
    op.IHostInterfaces: 'onode.host_interfaces',
    op.IListVMS: 'onode.vm_list_vms',
    op.IRebootVM: 'onode.vm_reboot_vm',
    op.IResumeVM: 'onode.vm_resume_vm',
    op.IShutdownVM: 'onode.vm_shutdown_vm',
    op.IStartVM: 'onode.vm_start_vm',
    op.ISuspendVM: 'onode.vm_suspend_vm',
    op.IUndeployVM: 'onode.vm_undeploy_vm',
    op.IMigrateVM: 'onode.vm_migrate',
    op.IUpdateVM: 'onode.host_update_vm'
}

OVERRIDE_EXECUTORS = {
    op.IDeployVM: AsynchronousSaltExecutor,
    op.IUndeployVM: AsynchronousSaltExecutor
}


# Avoid polluting the global namespace with temporary variables:
def _generate_classes():
    # Dynamically generate an adapter class for each supported Salt salttion:
    for interface, action in ACTIONS.items():
        cls_name = 'Salt%s' % interface.__name__[1:]
        cls = type(cls_name, (SaltBase, ), dict(action=action))
        classImplements(cls, interface)
        executor = get_config().getstring('salt', 'executor_class', 'sync')
        cls.__executor__ = OVERRIDE_EXECUTORS.get(interface, SaltBase.executor_classes[executor])
        globals()[cls_name] = cls
_generate_classes()
