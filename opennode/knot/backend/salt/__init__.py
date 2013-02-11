from __future__ import absolute_import

from grokcore.component import Adapter, context, baseclass
from twisted.internet import defer
from twisted.python import log
from zope.interface import classImplements
import json
import logging

from opennode.knot.backend import operation as op
from opennode.knot.backend import subprocess
from opennode.knot.model.compute import ISaltInstalled
from opennode.oms.config import get_config
from opennode.oms.zodb import db


class SimpleSaltExecutor(object):
    """ Simple executor implementation.
    NOTE: Ignores hard_timeout configuration parameter and obsoletes other parameters under salt section
    """

    @defer.inlineCallbacks
    def run(self):
        try:
            log.msg('Running action against "%s": %s args: %s' % (self.hostname, self.action, self.args),
                    system='salt', logLevel=logging.DEBUG)
            cmd = get_config().getstring('salt', 'remote_command', 'salt')
            output = yield subprocess.async_check_output(cmd.split(' ') +
                                 ['--no-color', '--out=json', self.hostname, self.action] +
                                 map(lambda s: '"%s"' % s, map(str, self.args)))
        except subprocess.CalledProcessError as e:
            log.msg('Failed action %s on host: %s (%s)' % (self.action, self.hostname, e),
                    system='salt-remote')
            raise
        else:
            data = json.loads(output) if output else {}
            rdata = self._handle_errors(data)
            defer.returnValue(rdata)

    def _handle_errors(self, data):
        hostkey = self.hostname if len(data.keys()) != 1 else data.keys()[0]

        if hostkey not in data:
            raise op.OperationRemoteError(msg='Remote returned empty response')

        if type(data[hostkey]) is str and data[hostkey].startswith('Traceback'):
            raise op.OperationRemoteError(msg="Remote error on %s:%s" % (hostkey, self.action),
                                          remote_tb=data[hostkey])

        if type(data[hostkey]) is str and data[hostkey].endswith('is not available.'):
            # TODO: mark the host as unmanageable (agent modules are missing)
            raise op.OperationRemoteError(msg="Remote error on %s: module (%s) unavailable" %
                                          (hostkey, self.action))

        return data[hostkey]


class SaltBase(Adapter):
    """Base class for all Salt method calls."""
    context(ISaltInstalled)
    baseclass()

    action = None
    __executor__ = None

    # 'sync' and 'async' are left for backwards compatibility with older configs
    executor_classes = {'sync': SimpleSaltExecutor,
                        'async': SimpleSaltExecutor,
                        'simple': SimpleSaltExecutor}

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
    op.IUpdateVM: 'onode.vm_update_vm'
}

OVERRIDE_EXECUTORS = {
}


# Avoid polluting the global namespace with temporary variables:
def _generate_classes():
    # Dynamically generate an adapter class for each supported Salt salttion:
    for interface, action in ACTIONS.items():
        cls_name = 'Salt%s' % interface.__name__[1:]
        cls = type(cls_name, (SaltBase, ), dict(action=action))
        classImplements(cls, interface)
        executor = get_config().getstring('salt', 'executor_class', 'simple')
        cls.__executor__ = OVERRIDE_EXECUTORS.get(interface, SaltBase.executor_classes[executor])
        globals()[cls_name] = cls
_generate_classes()
