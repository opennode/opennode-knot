from __future__ import absolute_import

from grokcore.component import context
from grokcore.component import Adapter, implements
from zope import schema
from zope.component import provideSubscriptionAdapter, provideAdapter
from zope.interface import Interface

import netaddr

from opennode.knot.model.common import IInVirtualizationContainer
from opennode.knot.model.console import Consoles
from opennode.knot.model.network import NetworkInterfaces, NetworkRoutes
from opennode.knot.model.template import Templates
from opennode.knot.model.zabbix import IZabbixConfiguration
from opennode.oms.config import get_config
from opennode.oms.model.location import ILocation
from opennode.oms.model.form import alsoProvides
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Container
from opennode.oms.model.model.base import IMarkable, IDisplayName
from opennode.oms.model.model.base import require_admins
from opennode.oms.model.model.search import ModelTags
from opennode.oms.model.model.stream import MetricsContainerExtension, IMetrics
from opennode.oms.model.schema import Path, RestrictedHostname
from opennode.oms.security.directives import permissions
from opennode.oms.security.authentication import sudo
from opennode.oms.util import adapter_value


M = 10 ** 6


class IManageable(Interface):
    """ Marker for any management stack installed on a compute """


class IFuncInstalled(IManageable):
    """Marker for FUNC-controlled Computes."""


class ISaltInstalled(IManageable):
    """Marker for SaltStack-controlled computes."""


class ICompute(Interface):
    # Network parameters
    hostname = RestrictedHostname(title=u"Host name", min_length=1)
    mac_address = schema.TextLine(title=u'MAC address', min_length=17, max_length=23, required=False,
                                  description=u'MAC address, formatted as a set of colon-separated '
                                  'hexadecimal octet values')
    ipv4_address = schema.TextLine(title=u"IPv4 address", min_length=7, required=False)
    ipv6_address = schema.TextLine(title=u"IPv6 address", min_length=6, required=False)
    nameservers = schema.List(title=u"Nameservers", description=u"IPs of DNS servers",
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
        title=u"State", values=(u'starting', u'active', u'stopping', u'inactive', u'suspended'),
        required=False, default=u'inactive')
    effective_state = schema.TextLine(
        title=u"Effective state", readonly=True, required=False)

    autostart = schema.Bool(title=u"Autostart", description=u"Start on boot", required=False)

    # Processing/network capabilities:
    num_cores = schema.Int(
        title=u"Num. Cores", description=u"Total number of cores across all CPUs",
        required=False)
    memory = schema.Int(
        title=u"RAM Size in MB", description=u"RAM size in MB",
        required=False)
    diskspace = schema.Dict(
        title=u"Disk size in MB", description=u"List of disk partition sizes",
        key_type=schema.TextLine(), value_type=schema.Float(),
        required=False, readonly=True)
    network = schema.Float(
        title=u"Network", description=u"Network bandwidth in Bps",
        required=False, readonly=True)
    swap_size = schema.Int(
        title=u"Swap Size in MB", description=u"Swap size",
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

    # availability
    last_ping = schema.Bool(title=u'Success of the last ping', required=False,
                            readonly=True, default=False)

    pingcheck = schema.List(title=u'Ping history', required=False,
                            readonly=True, default=[],
                            value_type=schema.Dict(title=u'Ping result',
                                                   required=False,
                                                   readonly=True))

    suspicious = schema.Bool(title=u'Suspicion of unavailability',
                             required=False, readonly=True, default=False)

    failure = schema.Bool(title=u'Availability failure', required=False,
                          readonly=True, default=False)

    agent_version = schema.TextLine(title=u'Agent version', required=False,
                                    readonly=True, default=u'')

    exclude_from_allocation = schema.Bool(title=u'Exclude from allocation', required=False, default=False)


class IVirtualCompute(Interface):
    """A virtual compute."""
    template = Path(title=u"Template", base_path='../templates/by-name/', relative_to=Path.PARENT)
    cpu_limit = schema.Float(title=u"CPU Limit", description=u"CPU usage limit", required=False)
    ctid = schema.Int(title=u'OpenVZ CTID', description=u'OpenVZ CTID (applies only to OpenVZ VMs)',
                      required=False, default=101)

    license_activated = schema.Bool(title=u'License activated', required=False, default=True,
                                    readonly=True)

    notify_admin = schema.Bool(title=u'Notify admin on deploy', required=False, default=False,
                               readonly=True)


class IInCompute(Interface):
    """Implementors of this interface can be contained in a `Compute` container."""


class IAllocating(Interface):
    """Marker interface implemented when the compute has an allocate operation in progress."""


class IDeployed(Interface):
    """Marker interface implemented when the compute has been deployed."""


class IUndeployed(Interface):
    """Marker interface implemented when the compute has not been deployed yet."""


class IDeploying(Interface):
    """Marker interface implemented when the compute has a deploy operation in progress."""


class Compute(Container):
    """A compute node."""

    implements(ICompute, IDisplayName, IMarkable, IInVirtualizationContainer)

    permissions(dict(hostname=('read', 'modify'),
                     mac_addr=('read', 'modify'),
                     ipv4_address=('read', 'zope.Security'),
                     ipv6_address=('read', 'modify'),
                     nameservers=('read', 'modify'),
                     dns_domains=('read', 'modify'),
                     architecture=('read', 'modify'),
                     cpu_info=('read', 'modify'),
                     os_release=('read', 'modify'),
                     kernel=('read', 'modify'),
                     state=('read', 'modify'),
                     effective_state=('read', 'modify'),
                     num_cores=('read', 'modify'),
                     memory=('read', 'modify'),
                     diskspace=('read', 'modify'),
                     network=('read', 'modify'),
                     uptime=('read', 'modify'),
                     swap_size=('read', 'modify'),
                     cpu_usage=('read'),
                     memory_usage=('read'),
                     diskspace_usage=('read'),
                     network_usage=('read'),
                     cpu_limit=('read', 'modify'),
                     template=('read', 'modify'),
                     autostart=('read', 'modify'),
                     ctid=('read', 'modify'),
                     exclude_from_allocation=('read', 'modify'),
                     license_activated=('read', 'zope.Security')
                     ))

    __contains__ = IInCompute

    __markers__ = [IVirtualCompute, IDeployed, IUndeployed, IDeploying, IZabbixConfiguration, IManageable,
                   ISaltInstalled, IFuncInstalled, IAllocating]

    hostname = u''

    mac_address = None
    _ipv4_address = u'0.0.0.0/32'
    ipv6_address = u'::/128'
    nameservers = [u'8.8.8.8']
    dns_domains = []

    architecture = (u'x86_64', u'linux', u'centos')
    cpu_info = u"unknown"

    os_release = u"build 35"
    kernel = u"unknown"
    last_ping = False
    pingcheck = []
    suspicious = False
    failure = False

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

    # empty values for metrics
    uptime = None
    cpu_usage = (0.0, 0.0, 0.0)
    memory_usage = 0.0
    network_usage = (0.0, 0.0)
    diskspace_usage = {
        u'root': 0.0,
        u'boot': 0.0,
        u'storage': 0.0,
    }

    cpu_limit = 1.0

    autostart = False

    # zabbix specific
    zabbix_hostgroups = []
    zabbix_dns_name = None
    zabbix_ipv4_address = None
    zabbix_use_dns = True
    zabbix_agent_port = 10050

    agent_version = u''

    exclude_from_allocation = False

    license_activated = True

    notify_admin = False

    def __init__(self, hostname, state=None, memory=None, template=None, ipv4_address=None, mgt_stack=None):
        super(Compute, self).__init__()

        self.hostname = hostname
        self.memory = memory
        self.state = state
        self.template = template
        if ipv4_address:
            self._ipv4_address = ipv4_address
        self._mgt_stack = mgt_stack

        if self.template:
            alsoProvides(self, IVirtualCompute)
            alsoProvides(self, IUndeployed)
        elif self._mgt_stack:
            alsoProvides(self, self._mgt_stack)

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

    @property
    def effective_state(self):
        """Since we lack schema/data upgrade scripts I have to
        resort on this tricks to cope with the fact that I have
        existing objects around in the several test dbs, and branches.

        """
        return getattr(self, '_effective_state', unicode(self.state))

    def get_ctid(self):
        return getattr(self, '_ctid', None)

    def set_ctid(self, value):
        self._ctid = int(value) if value is not None else None

    ctid = property(get_ctid, set_ctid)

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

    def get_ipv4_address(self):
        if 'interfaces' not in self._items:
            return self._ipv4_address

        primaries = [i.ipv4_address for i in self._items['interfaces']
                     if i.ipv4_address and getattr(i, 'primary', False)]

        if primaries:
            return unicode(primaries[0])

        # No interface has been marked as primary, so let's just pick one
        addresses = [i.ipv4_address for i in self._items['interfaces'] if i.ipv4_address]
        if not addresses:
            return self._ipv4_address
        return unicode(addresses[0])

    @require_admins
    def set_ipv4_address_fallback(self, value):
        """ Sets the fallback value of the IP address """
        self._ipv4_address = value

    ipv4_address = property(get_ipv4_address, set_ipv4_address_fallback)

    def __repr__(self):
        return '<Compute %s>' % self.__name__


class ComputeTags(ModelTags):
    context(Compute)

    def auto_tags(self):
        res = [u'state:' + self.context.state] if self.context.state else []
        if self.context.architecture:
            for i in self.context.architecture:
                res.append(u'arch:' + i)

        from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
        p = sudo(self.context)
        if (IVirtualCompute.providedBy(p) and
                IVirtualizationContainer.providedBy(p.__parent__)):
            res.append(u'virt_type:' + p.__parent__.backend)
            res.append(u'virt:yes')
        else:
            res.append(u'virt:no')

        config = get_config()
        if config.has_section('netenv-tags'):
            for tag, nets in config.items('netenv-tags'):
                try:
                    if (self.context.ipv4_address is not None and
                        len(netaddr.all_matching_cidrs(self.context.ipv4_address.split('/')[0],
                                                       nets.split(','))) > 0):
                        res.append(u'env:' + tag)
                except ValueError:
                    # graceful ignoring of incorrect ips
                    pass
        return res


class VirtualComputeLocation(Adapter):
    implements(ILocation)
    context(IVirtualCompute)

    def get_url(self):
        return '/computes/%s/' % (self.context.__name__)


provideAdapter(adapter_value(['cpu_usage', 'memory_usage', 'network_usage', 'diskspace_usage']),
               adapts=(Compute, ), provides=IMetrics)


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(Compute, ))
provideSubscriptionAdapter(MetricsContainerExtension, adapts=(Compute, ))
