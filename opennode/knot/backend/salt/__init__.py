from __future__ import absolute_import

from grokcore.component import Adapter, context, baseclass
from twisted.internet import defer, reactor
from twisted.python import log
from zope.interface import classImplements

import cPickle
import json
import logging
import multiprocessing
import time

from opennode.knot.backend import operation as op
from opennode.knot.backend import subprocess
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.config import get_config
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
        except Exception as e:
            log.err(system='salt-local')
            self.q.put(cPickle.dumps({'_error': e}))


class SimpleSaltExecutor(object):
    """ Simple executor implementation.
    NOTE: Ignores hard_timeout configuration parameter and obsoletes other parameters under salt section
    """
    def __init__(self, hostname, action, interaction, timeout=None):
        self.hostname = hostname
        self.action = action
        self.interaction = interaction
        self.timeout = timeout

    @defer.inlineCallbacks
    def run(self, *args, **kwargs):
        self.args = args
        log.msg('Running action against "%s": %s args: %s timeout: %s' % (self.hostname, self.action,
                                                                          self.args, self.timeout),
                system='salt-simple', logLevel=logging.DEBUG)
        cmd = get_config().getstring('salt', 'remote_command', 'salt')
        output = yield subprocess.async_check_output(
            filter(None, (cmd.split(' ') +
                          ['--no-color', '--out=json',
                           ('--timeout=%s' % self.timeout) if self.timeout is not None else None,
                           self.hostname, self.action] +
                          map(lambda s: '"%s"' % s, map(str, self.args)))))
        data = json.loads(output) if output else {}
        rdata = self._handle_errors(data)
        defer.returnValue(rdata)

    def _handle_errors(self, data):
        hostkey = self.hostname if len(data.keys()) != 1 else data.keys()[0]

        if hostkey not in data:
            raise op.OperationRemoteError(msg='Remote returned empty response')

        if type(data[hostkey]) in (str, unicode) and data[hostkey].startswith('Traceback'):
            raise op.OperationRemoteError(msg="Remote error on %s:%s" % (hostkey, self.action),
                                          remote_tb=data[hostkey])

        if type(data[hostkey]) in (str, unicode) and data[hostkey].endswith('is not available.'):
            # TODO: mark the host as unmanageable (agent modules are missing)
            raise op.OperationRemoteError(msg="Remote error on %s: module (%s) unavailable" %
                                          (hostkey, self.action))

        return data[hostkey]


class SaltExecutor(object):

    def _get_client(self):
        """Returns an instance of Salt Stack LocalClient."""
        if getattr(self, 'client', None) is None:
            self.client = SaltMultiprocessingClient()
        return self.client

    def _destroy_client(self):
        if getattr(self, 'client', None) is not None:
            self.client = None


class AsynchronousSaltExecutor(SaltExecutor):
    interval = 0.1

    def __init__(self, hostname, action, interaction):
        self.hostname = hostname
        self.action = action
        self.interaction = interaction
        self.hard_timeout = get_config().getint('salt', 'hard_timeout')

    def run(self, *args, **kwargs):
        self.deferred = defer.Deferred()

        client = self._get_client()
        client.provide_args(self.hostname, self.action, args)
        client.start()

        self.starttime = time.time()
        reactor.callLater(self.interval, self.poll)

        return self.deferred

    def poll(self):
        done = not self._get_client().is_alive()
        now = time.time()

        if done:
            pdata = self._get_client().q.get()
            results = cPickle.loads(pdata)
            self._fire_events(results)
            return
        elif self.starttime + self.hard_timeout < now:
            log.msg("Timeout while executing '%s' @ '%s'" % (self.action, self.hostname),
                    system='salt-async')
            self._destroy_client()
            self.deferred.errback(op.OperationRemoteError(msg='Timeout waiting for response from %s (%s)' %
                                                          (self.hostname, self.action)))
            return

        reactor.callLater(self.interval, self.poll)

    def _fire_events(self, data):
        hostkey = self.hostname if len(data.keys()) != 1 else data.keys()[0]

        if '_error' in data:
            self.deferred.errback(data['_error'])
        elif hostkey not in data:
            self.deferred.errback(op.OperationRemoteError(msg='Remote returned empty response'))
        elif type(data[hostkey]) is str and data[hostkey].startswith('Traceback'):
            self.deferred.errback(op.OperationRemoteError(msg="Remote error on %s:%s" % (hostkey,
                                                                                         self.action),
                                                          remote_tb=data[hostkey]))
        elif type(data[hostkey]) is str and data[hostkey].endswith('is not available.'):
            # TODO: mark the host as suspicious/failed (agent modules are missing)
            self.deferred.errback(op.OperationRemoteError(msg="Remote error on %s: module (%s) unavailable" %
                                                          (hostkey, self.action)))
        else:
            self.deferred.callback(data[hostkey])


class SaltBase(Adapter):
    """Base class for all Salt method calls."""
    context(ISaltInstalled)
    baseclass()

    action = None
    __executor__ = None

    # 'sync' and 'async' are left for backwards compatibility with older configs
    executor_classes = {'sync': SimpleSaltExecutor,
                        'async': AsynchronousSaltExecutor,
                        'simple': SimpleSaltExecutor}

    @defer.inlineCallbacks
    def run(self, *args, **kwargs):
        executor_class = self.__executor__
        hostname = yield op.IMinion(self.context).hostname()
        interaction = db.context(self.context).get('interaction', None)
        executor = executor_class(hostname, self.action, interaction, timeout=self.timeout)
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
    op.IUpdateVM: 'onode.vm_update_vm',
    op.IPing: 'test.ping',
    op.IAgentVersion: 'test.version',
    op.IInstallPkg: 'pkg.install'
}


TIMEOUTS = {
    op.IMigrateVM: 3600,
    op.IDeployVM: 600,
}


OVERRIDE_EXECUTORS = {
}


# TODO: support for 'remote Salt' configuration
@defer.inlineCallbacks
def get_master_version():
    output = yield subprocess.async_check_output(['salt-master', '--version'])
    version = output.strip(' \n').split(' ')[1]
    defer.returnValue(version)


# Avoid polluting the global namespace with temporary variables:
def _generate_classes():
    # Dynamically generate an adapter class for each supported Salt salt Action:
    for interface, action in ACTIONS.items():
        cls_name = 'Salt%s' % interface.__name__[1:]
        cls = type(cls_name, (SaltBase, ), dict(action=action))
        classImplements(cls, interface)
        executor = get_config().getstring('salt', 'executor_class', 'simple')
        cls.__executor__ = OVERRIDE_EXECUTORS.get(interface, SaltBase.executor_classes[executor])
        cls.timeout = TIMEOUTS.get(interface)
        globals()[cls_name] = cls


_generate_classes()
