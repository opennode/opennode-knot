from grokcore.component import implements

from opennode.oms.model.model.plugins import IPlugin, PluginInfo


class KnotPlugin(PluginInfo):
    implements(IPlugin)

    def initialize(self):
        print "[KnotPlugin] initializing plugin"

