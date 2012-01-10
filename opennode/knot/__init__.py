from grokcore.component import implements, Subscription, context

import opennode.knot
from opennode.oms.config import IRequiredConfigurationFiles, gen_config_file_names
from opennode.oms.model.model.plugins import IPlugin, PluginInfo


class KnotRequiredConfigurationFiles(Subscription):
    implements(IRequiredConfigurationFiles)
    context(object)

    def config_file_names(self):
        return gen_config_file_names(opennode.knot, 'knot')


class KnotPlugin(PluginInfo):
    implements(IPlugin)

    def initialize(self):
        print "[KnotPlugin] initializing plugin"

