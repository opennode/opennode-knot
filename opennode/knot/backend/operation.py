from zope.interface import Interface


class OperationRemoteError(Exception):
    """ Raised on any error coming from Salt"""
    def __init__(self, msg="Unspecified error", remote_tb=""):
        super(OperationRemoteError, self).__init__(msg)
        self.remote_tb = remote_tb


class IJob(Interface):

    def run():
        pass

    def start_polling():
        pass


class IMinion(Interface):
    def hostname():
        """Return a (possibly deferred) hostname of the minion"""


class IBotoManageable(Interface):
    """Marker for Computes controlled through the boto library."""


class IGetComputeInfo(IJob):
    """Returns general information about a compute (os, architecture, devices, etc)."""


class IHostInterfaces(IJob):
    """Returns detailed info about host interfaces. hardware.info doesn't work on all archs."""


class IGetRoutes(IJob):
    """Returns route info"""


class IGetGuestMetrics(IJob):
    """Returns guest VM metrics."""


class IGetHostMetrics(IJob):
    """Returns host (PHY) metrics."""


class IGetDiskUsage(IJob):
    """Returns disk usage."""


class IGetLocalTemplates(IJob):
    """Get local templates"""


class IGetVirtualizationContainers(IJob):
    """Get virtualization container provided by a compute"""


class IDeployVM(IJob):
    """Deploys a vm."""


class IUndeployVM(IJob):
    """Undeploys a vm."""


class IListVMS(IJob):
    """List vms"""


class IStartVM(IJob):
    """Starts a vm."""


class IShutdownVM(IJob):
    """Shuts down a vm."""


class IDestroyVM(IJob):
    """Destroys a vm."""


class ISuspendVM(IJob):
    """Suspends a vm."""


class IResumeVM(IJob):
    """Resumes a vm."""


class IRebootVM(IJob):
    """Reboots a vm."""


class IGetSignedCertificateNames(IJob):
    """Get accepted cert names"""


class IGetIncomingHosts(IJob):
    """Retrieve requests for new managed servers"""


class IAcceptIncomingHost(IJob):
    """Accept request a new managed server"""


class ICleanupHost(IJob):
    """Cleanup server leftovers"""


class IGetHWUptime(IJob):
    """Return uptime of a physical host"""


class IMigrateVM(IJob):
    """ Migrate compute to another host """


class IUpdateVM(IJob):
    """Update VM configuration"""


class IPing(IJob):
    """ Test agent/host connectivity """


class IAgentVersion(IJob):
    """ Agent-specific version checking """


class IInstallPkg(IJob):
    """ Install a package using system package manager (e.g. yum) """


class IGetOwner(IJob):
    """ Pull ownership information from the VM config """


class ISetOwner(IJob):
    """ Push ownership information to the VM config """
