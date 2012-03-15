from __future__ import absolute_import

from grokcore.component import context
from zope import schema
from zope.component import provideSubscriptionAdapter, provideAdapter
from zope.interface import Interface, implements, alsoProvides


from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import IContainer, Container, AddingContainer, IDisplayName, ContainerInjector
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.knot.model.console import Consoles
from opennode.knot.model.network import NetworkInterfaces, NetworkRoutes
from opennode.oms.model.model.search import ModelTags
from opennode.knot.model.template import Templates
from opennode.oms.model.model.stream import MetricsContainerExtension, IMetrics
from opennode.oms.model.model.symlink import Symlink
from opennode.knot.backend.operation import IFuncInstalled
from opennode.oms.model.schema import Path
from opennode.oms.security.directives import permissions
from opennode.oms.util import adapter_value


M = 10 ** 6


class ICompute(Interface):
    # Network parameters
    hostname = schema.TextLine(
        title=u"Host name", min_length=3)
    ipv4_address = schema.TextLine(
        title=u"IPv4 address", min_length=7, required=False)
    ipv6_address = schema.TextLine(
        title=u"IPv6 address", min_length=6, required=False)
    nameservers = schema.List(
        title=u"Nameservers", description=u"IPs of DNS servers",
        value_type=schema.TextLine(), required=False)
    dns_domains = schema.List(
        title=u"DNS Domains", description=u"Domain names used for DNS host name lookup",
        value_type=schema.TextLine(), required=False)

    # Hardware/platform info
    architecture = schema.Tuple(
        title=u"Architecture", description=u"OS arch, OS type, OS distribution/flavour",
        value_type=schema.TextLine(), max_length=3, min_length=3,
        required=False, readonly=True)
    cpu_info = schema.TextLine(
        title=u"CPU Info", description=u"Info about the CPU such as model, speed in Hz, cache size",
        required=False, readonly=True)
    os_release = schema.TextLine(
        title=u"OS Release", description=u"OS version info",
        required=False, readonly=True)
    kernel = schema.TextLine(
        title=u"Kernel", description=u"Kernel version (if applicable)",
        required=False, readonly=True)

    # State
    state = schema.Choice(
        title=u"State", values=(u'active', u'inactive', u'suspended'))
    effective_state = schema.TextLine(
        title=u"Effective state", readonly=True, required=False)

    # Processing/network capabilities:
    num_cores = schema.Int(
        title=u"Num. Cores", description=u"Total number of cores across all CPUs",
        required=False)
    memory = schema.Int(
        title=u"RAM Size", description=u"RAM size in MB",
        required=False)
    diskspace = schema.Dict(
        title=u"Disk size", description=u"List of disk partition sizes",
        key_type=schema.TextLine(), value_type=schema.Float(),
        required=False, readonly=True)
    network = schema.Float(
        title=u"Network", description=u"Network bandwidth in Bps",
        required=False, readonly=True)
    swap_size = schema.Int(
        title=u"Swap Size", description=u"Swap size",
        required=False)

    # Resource utilization/load:
    uptime = schema.Float(
        title=u"Uptime", description=u"Uptime in seconds",
        required=False, readonly=True)
    cpu_usage = schema.Tuple(
        title=u"CPU Load", description=u"CPU load during the past 1, 5 and 15 minutes",
        value_type=schema.Float(),
        required=False, readonly=True)
    memory_usage = schema.Float(
        title=u"Memory Usage", description=u"Memory usage in MB",
        required=False, readonly=True)
    diskspace_usage = schema.Dict(
        title=u"Diskspace Utilization", description=u"List of disk partition usages",
        key_type=schema.TextLine(), value_type=schema.Float(),
        required=False, readonly=True)
    network_usage = schema.Tuple(
        title=u"Network Load", description=u"Network load in B/s (incoming and outgoing)",
        value_type=schema.Float(),
        required=False, readonly=True)

    # VM only
    template = Path(title=u"Template", base_path='../templates/by-name/', relative_to=Path.PARENT)
    cpu_limit = schema.Float(title=u"CPU Limit", description=u"CPU usage limit", required=False)


class IInCompute(Interface):
    """Implementors of this interface can be contained in a `Compute` container."""


class IDeployed(Interface):
    """Marker interface implemented when the compute has been deployed."""


class IUndeployed(Interface):
    """Marker interface implemented when the compute has not been deployed yet."""


class Compute(Container):
    """A compute node."""

    implements(ICompute, IDisplayName)
    permissions(dict(hostname = ('read', 'modify'),
                     ipv4_address = ('read', 'modify'),
                     ipv6_address = ('read', 'modify'),
                     nameservers = ('read', 'modify'),
                     dns_domains = ('read', 'modify'),
                     architecture = ('read', 'modify'),
                     cpu_info = ('read', 'modify'),
                     os_release = ('read', 'modify'),
                     kernel = ('read', 'modify'),
                     state = ('read', 'modify'),
                     effective_state = ('read', 'modify'),
                     num_cores = ('read', 'modify'),
                     memory = ('read', 'modify'),
                     diskspace = ('read', 'modify'),
                     network = ('read', 'modify'),
                     uptime = ('read', 'modify'),
                     swap_size = ('read', 'modify'),
                     cpu_usage = ('read'),
                     memory_usage = ('read'),
                     diskspace_usage = ('read'),
                     network_usage = ('read'),
                     cpu_limit = ('read', 'modify'),
                     template = ('read', 'modify'),
                     autostart = ('read', 'modify'),
                     ))

    __contains__ = IInCompute

    _ipv4_address = u'0.0.0.0/32'
    ipv6_address = u'::/128'
    nameservers = []
    dns_domains = []

    architecture = (u'x86_64', u'linux', u'centos')
    cpu_info = u"unknown"

    os_release = u"build 35"
    kernel = u"unknown"

    num_cores = 1
    memory = 2048,
    network = 12.5 * M  # bytes
    diskspace = {
        u'total': 2000.0,
        u'/': 500.0,
        u'/boot': 100.0,
        u'/storage': 1000.0,
    }
    swap_size = 4192

    uptime = None
    cpu_usage = (0.1, 0.11, 0.14)
    memory_usage = 773.2
    network_usage = (5.2 * M, 1.9 * M)
    diskspace_usage = {
        u'root': 249.0,
        u'boot': 49.3,
        u'storage': 748.3,
    }

    cpu_limit = 1.0

    autostart = False

    def __init__(self, hostname, state, memory=None, template=None, ipv4_address=None):
        super(Compute, self).__init__()

        self.hostname = hostname
        self.memory = memory
        self.state = state
        self.template = template
        if ipv4_address:
            self._ipv4_address = ipv4_address

        if self.template:
            alsoProvides(self, IVirtualCompute)
        else:
            alsoProvides(self, IFuncInstalled)

        alsoProvides(self, IUndeployed)

        assert self.hostname

    def display_name(self):
        return self.hostname.encode('utf-8')

    @property
    def nicknames(self):
        """Returns all the nicknames of this Compute instance.

        Nicknames can be used to traverse to this object using
        alternative, potentially more convenient and/more memorable,
        names.

        """
        return [self.hostname, ]

    def get_effective_state(self):
        """Since we lack schema/data upgrade scripts I have to
        resort on this tricks to cope with the fact that I have
        existing objects around in the several test dbs, and branches.

        """
        return getattr(self, '_effective_state', unicode(self.state))

    def set_effective_state(self, value):
        self._effective_state = value

    effective_state = property(get_effective_state, set_effective_state)

    def __str__(self):
        return 'compute%s' % self.__name__

    def get_consoles(self):
        if 'consoles' not in self._items:
            self._add(Consoles())
        return self._items['consoles']

    def set_consoles(self, value):
        if 'consoles' in self._items:
            del self._items['consoles']
        self._add(value)

    consoles = property(get_consoles, set_consoles)

    @property
    def templates(self):
        from opennode.oms.zodb import db

        @db.assert_transact
        def do_it():
            if not self['templates']:
                templates = Templates()
                templates.__name__ = 'templates'
                self._add(templates)
            return self['templates']
        return do_it()

    def get_interfaces(self):
        if 'interfaces' not in self._items:
            self._add(NetworkInterfaces())
        return self._items['interfaces']

    def set_interfaces(self, value):
        if 'interfaces' in self._items:
            del self._items['interfaces']
        self._add(value)

    interfaces = property(get_interfaces, set_interfaces)

    def get_routes(self):
        if 'routes' not in self._items:
            self._add(NetworkRoutes())
        return self._items['routes']

    def set_routes(self, value):
        if 'routes' in self._items:
            del self._items['routes']
        self._add(value)

    routes = property(get_routes, set_routes)

    @property
    def ipv4_address(self):
        if 'interfaces' not in self._items:
            return self._ipv4_address
        addresses = [i.ipv4_address for i in self._items['interfaces'] if i.ipv4_address]
        if not addresses:
            return self._ipv4_address
        return unicode(addresses[0])


class ComputeTags(ModelTags):
    context(Compute)

    def auto_tags(self):
        res = [u'state:' + self.context.state]
        if self.context.architecture:
            for i in self.context.architecture:
                res.append(u'arch:' + i)

        from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
        if IVirtualCompute.providedBy(self.context) and IVirtualizationContainer.providedBy(self.context.__parent__):
            res.append(u'virt_type:' + self.context.__parent__.backend)

        return res


class IVirtualCompute(Interface):
    """A virtual compute."""

    autostart = schema.Bool(title=u"Autostart", description=u"Start on boot", required=False)


class Computes(AddingContainer):
    __contains__ = Compute
    __name__ = 'computes'

    def __str__(self):
        return 'Compute list'

    @property
    def _items(self):
        # break an import cycle
        from opennode.oms.zodb import db
        machines = db.get_root()['oms_root']['machines']

        computes = {}

        def collect(container):
            for item in container.listcontent():
                if ICompute.providedBy(item):
                    computes[item.__name__] = Symlink(item.__name__, item)
                if IContainer.providedBy(item):
                    collect(item)

        collect(machines)
        return computes

    def _add(self, item):
        # break an import cycle
        from opennode.oms.zodb import db
        machines = db.get_root()['oms_root']['machines']
        return (machines.hangar if IVirtualCompute.providedBy(item) else machines).add(item)

    def __delitem__(self, key):
        item = self._items[key]
        if isinstance(item, Symlink):
            del item.target.__parent__[item.target.__name__]


class ComputesRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Computes


provideAdapter(adapter_value(['cpu_usage', 'memory_usage', 'network_usage', 'diskspace_usage']), adapts=(Compute,), provides=(IMetrics))


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(Compute, ))
provideSubscriptionAdapter(ByNameContainerExtension, adapts=(Computes, ))
provideSubscriptionAdapter(MetricsContainerExtension, adapts=(Compute, ))
