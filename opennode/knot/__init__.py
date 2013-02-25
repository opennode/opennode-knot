import logging

from grokcore.component import implements, Subscription, context

import opennode.knot

from opennode.oms.config import IRequiredConfigurationFiles, gen_config_file_names
from opennode.oms.model.model import creatable_models
from opennode.oms.model.model.plugins import IPlugin, PluginInfo

from opennode.knot.model.compute import Compute
from opennode.knot.model.template import Template
from opennode.knot.model.virtualizationcontainer import VirtualizationContainer
from opennode.knot.model.hangar import Hangar
from opennode.knot.model.storage import Storage
from opennode.knot.model.network import Network, NetworkInterface
from opennode.knot.model.console import VncConsole

log = logging.getLogger(__name__)

class KnotRequiredConfigurationFiles(Subscription):
    implements(IRequiredConfigurationFiles)
    context(object)

    def config_file_names(self):
        return gen_config_file_names(opennode.knot, 'knot')


class KnotPlugin(PluginInfo):
    implements(IPlugin)

    def initialize(self):
        log.info("initializing KNOT plugin")

        compute_creatable_models = dict((cls.__name__.lower(), cls)
                                        for cls in [Compute, Template, Network, NetworkInterface, Storage,
                                                    VirtualizationContainer, Hangar, VncConsole])

        creatable_models.update(compute_creatable_models)
