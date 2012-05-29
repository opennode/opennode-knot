from zope import schema
from zope.interface import Interface


class IZabbixConfiguration(Interface):
    """Configuration for registration of the Compute in Zabbix."""

    zabbix_hostgroups = schema.List(title=u"Host groups", description=u"Host groups this compute belongs to",
                            value_type=schema.TextLine(), required=True)

    zabbix_dns_name = schema.TextLine(title=u"Zabbix agent's DNS name", min_length=1, required=False)
    zabbix_ipv4_address = schema.TextLine(title=u"Zabbix agent's IPv4 address", min_length=7, required=False)

    zabixx_agent_port = schema.Bool(title=u"Zabbix agent port", description=u"Port where zabbix agent is listening",
        required=False, min=1, max=65535)

    zabbix_use_dns = schema.Bool(title=u'Use DNS',
        description=u'True if DNS entry of zabbix agent should be used for contacting the agent')
