from __future__ import absolute_import

import netaddr

from grokcore.component import context
from zope import schema
from zope.schema.interfaces import IFromUnicode, IInt
from zope.interface import Interface, implements, implementer

from opennode.oms.model.model.base import ReadonlyContainer, Container, Model
from opennode.oms.model.model.base import ContainerInjector
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.model.model.symlink import Symlink


class INetworkInterface(Interface):
    name = schema.TextLine(title=u"Interface name", min_length=3)
    hw_address = schema.TextLine(title=u"MAC", min_length=17)
    state = schema.Choice(title=u"State", values=(u'active', u'inactive'))
    ipv4_address = schema.TextLine(title=u"IPv4 network address", min_length=7, required=False)
    ipv6_address = schema.TextLine(title=u"IPv6 network address", min_length=7, required=False)

    metric = schema.Int(title=u"Metric")
    bcast = schema.TextLine(title=u"Broadcast")
    stp = schema.Bool(title=u"STP enabled")
    rx = schema.TextLine(title=u"RX bytes")
    tx = schema.TextLine(title=u"TX bytes")

    primary = schema.Bool(title=u"Primary interface")


class IBridgeInterface(INetworkInterface):
    members = schema.List(title=u"Bridge members", required=False, readonly=True)


class NetworkInterface(ReadonlyContainer):
    implements(INetworkInterface)

    def __init__(self, name, network, hw_address, state):
        self.__name__ = name
        self.name = name
        self.hw_address = hw_address
        self.state = state
        self.network = network

        self.metric = 1
        self.tx = ''
        self.rx = ''
        self.stp = False

        self.ipv6_address = ''
        self.ipv4_address = ''

        self.primary = False

    @property
    def _items(self):
        if self.network:
            return {'network': Symlink('network', self.network)}
        return {}

    @property
    def bcast(self):
        if not self.ipv4_address:
            return None

        if '/' in self.ipv4_address:
            ip, prefix = self.ipv4_address.split('/')
        else:
            raise ValueError('ipv4_address must be a string in full IPv4 address format: 0.0.0.0/32')

        l = 0
        for b in ip.split('.'):
            l = l << 8 | int(b)
        mask = 0xffffffff >> int(prefix)
        l = l | mask
        o = []
        for i in xrange(0, 4):
            o.insert(0, l & 0xff)
            l = l >> 8
        return '.'.join(str(i) for i in o)


class BridgeInterface(NetworkInterface):
    implements(IBridgeInterface)

    def __init__(self, *args):
        super(BridgeInterface, self).__init__(*args)

        self.members = []

    @property
    def _items(self):
        res = super(BridgeInterface, self)._items
        # TODO: add symlinks for bridge members
        return res


class NetworkInterfaces(Container):
    __contains__ = INetworkInterface

    __name__ = 'interfaces'


class INetworkRoute(Interface):
    destination = schema.TextLine(title=u"Destination", min_length=7, required=True)
    gateway = schema.TextLine(title=u"Gateway", min_length=7, required=True)
    flags = schema.TextLine(title=u"Flags", required=True)
    metrics = schema.Int(title=u"Metrics", required=True)


class NetworkRoute(Container):
    implements(INetworkRoute)

    @property
    def nicknames(self):
        return [self.destination, self.gateway, self.flags, str(self.metrics)]


class NetworkRoutes(Container):
    __contains__ = INetworkRoute

    __name__ = 'routes'


class INetwork(Interface):
    state = schema.Choice(title=u"State", values=(u'active', u'inactive'))
    ipv4_address = schema.TextLine(title=u"IPv4 network address", min_length=7)
    ipv4_gateway = schema.TextLine(title=u"IPv4 Gateway", min_length=7)
    ipv4_address_range = schema.TextLine(title=u"IPv4 Range", min_length=7, required=False)
    ipv6_address = schema.TextLine(title=u"IPv6 network address", min_length=7, required=False)
    ipv6_gateway = schema.TextLine(title=u"IPv6 Gateway", min_length=6, required=False)
    ipv6_address_range = schema.TextLine(title=u"IPv6 Range", min_length=7, required=False)

    vlan = schema.TextLine(title=u"VLan", required=False)
    label = schema.TextLine(title=u"Label", required=False)


class Network(Model):
    implements(INetwork)

    def __init__(self, state):
        self.state = state

        self.vlan = None
        self.label = None

        self.ipv4_address = None
        self.ipv4_gateway = None
        self.ipv4_address_range = None
        self.ipv6_address = None
        self.ipv6_gateway = None
        self.ipv6_address_range = None

        self.allocation = None
        self.devices = []


class Networks(Container):
    __contains__ = INetwork

    __name__ = 'networks'


class IPAddressStorable(netaddr.IPAddress):

    def __init__(self, parent, *args, **kw):
        super(IPAddressStorable, self).__init__(*args, **kw)
        self.__name__ = str(self)
        self.__parent__ = parent


@implementer(IFromUnicode, IInt)
class IPAddressField(schema.Orderable, schema.Field):
    __doc__ = 'IPv4 address field'
    _type = int

    def __init__(self, *args, **kw):
        self._init_field = True
        super(IPAddressField, self).__init__(*args, **kw)
        self._init_field = False

    def _validate(self, value):
        if self._init_field:
            return
        return netaddr.IPAddress(value)

    def fromUnicode(self, value):
        v = self._validate(value)
        return v


class IIPv4Pool(Interface):
    name = schema.TextLine(title=u'Pool name')
    minimum = IPAddressField(title=u'Minimum IP')
    maximum = IPAddressField(title=u'Maximum IP')


# NOTE: [minimum .. maximum] specifies a contiguous range of IP addresses.
# It is up to the user to exclude any special IP addresses from the range
# (gateway and broadcast addresses, for example).
class IPv4Pool(Container):
    implements(IIPv4Pool)
    __contains__ = netaddr.IPAddress

    def __init__(self, name='ippool', min_ip=0, max_ip=0xffffffff):
        assert min_ip <= max_ip, 'Minimum IP value must be smaller or equal to max IP value'
        self.name = name
        self.__name__ = name
        self.minimum = netaddr.IPAddress(min_ip)
        self.maximum = netaddr.IPAddress(max_ip)

    def allocate(self):
        """ Search through the range to find first unallocated IP and mark it as used and return it"""
        for ip in xrange(int(self.minimum), int(self.maximum)):
            ip = netaddr.IPAddress(ip)
            if str(ip) not in self._items:
                self.use(ip)
                return ip

    def get(self, ip):
        return self._items.get(int(ip))

    def use(self, ip):
        self._items[int(ip)] = IPAddressStorable(self, int(ip))

    def free(self, ip):
        del self._items[int(ip)]

    def validate(self):
        assert int(self.minimum) <= int(self.maximum),\
                'Minimum IP value must be smaller or equal to max IP value'


class IPv4Pools(Container):
    __contains__ = IPv4Pool
    __name__ = 'ippools'

    def find_pool(self, ip):
        ip = netaddr.IPAddress(ip)
        for n, pool in self._items.iteritems():
            if int(pool.minimum) <= int(ip) and int(pool.maximum) >= int(ip):
                return pool

    def find_intersections(self, pool):
        for n, epool in self._items.iteritems():
            if int(pool.minimum) <= int(epool.maximum) and int(pool.maximum) >= int(epool.minimum):
                return True
        return False

    def add(self, pool):
        if self.find_intersections(pool):
            raise ValueError('IP ranges must not intersect')
        pool.validate()
        return super(IPv4Pools, self).add(pool)

    def allocate(self):
        for n, p in self._items.iteritems():
            ip = p.allocate()
            if ip is not None:
                return ip

class IPv4PoolsRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = IPv4Pools
