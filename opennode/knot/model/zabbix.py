from zope import schema
from zope.interface import Interface, implements
from zope.component import provideSubscriptionAdapter

from grokcore.component import context

from opennode.oms.model.model.base import Model, Container, ReadonlyContainer, IDisplayName, ContainerInjector
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.root import OmsRoot

from opennode.oms.config import get_config


class IZabbixConfiguration(Interface):
    """Configuration for registration of the Compute in Zabbix."""

    zabbix_hostgroups = schema.Dict(title=u"Host groups", description=u"Host groups this compute belongs to",
                                    key_type=schema.Int(), value_type=schema.TextLine(), required=False)

    zabbix_dns_name = schema.TextLine(title=u"Zabbix agent's DNS name", min_length=1, required=False)
    zabbix_ipv4_address = schema.TextLine(title=u"Zabbix agent's IPv4 address", min_length=7, required=False)

    zabbix_agent_port = schema.Int(title=u"Zabbix agent port", description=u"Port where zabbix agent is listening",
        required=False, min=1, max=65535)

    zabbix_use_dns = schema.Bool(title=u'Use DNS',
        description=u'True if DNS entry of zabbix agent should be used for contacting the agent')


class IZabbixServer(Interface):
    url = schema.TextLine(title=u"Zabbix server API url", min_length=2)
    username = schema.TextLine(title=u"Username", min_length=1)
    password = schema.TextLine(title=u"Password", min_length=1)

    zabbix_hostgroups = schema.Dict(title=u"Available host groups", description=u"Available Zabbix host groups",
                                    key_type=schema.Int(), value_type=schema.TextLine(), required=False)


class ZabbixServer(ReadonlyContainer):
    implements(IZabbixServer)

    def __init__(self, url, username, password, zabbix_hostgroups={}, category='primary'):
        self.__name__ = category
        self.url = url
        self.username = username
        self.password = password
        self.zabbix_hostgroups = zabbix_hostgroups

    @property
    def nicknames(self):
        return [self.url]


class ZabbixServers(Container):
    __name__ = 'zabbix'

    def __str__(self):
        return 'Zabbix servers'


class ZabbixServerRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = ZabbixServers


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(ZabbixServer, ))
