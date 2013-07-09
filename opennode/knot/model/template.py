from __future__ import absolute_import

from grokcore.component import context, implements
from zope import schema
from zope.component import provideSubscriptionAdapter
from zope.interface import Interface

from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Container
from opennode.oms.model.model.base import ContainerInjector
from opennode.oms.model.model.base import IDisplayName
from opennode.oms.model.model.base import Model
from opennode.oms.model.model.base import ReadonlyContainer
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.model.model.search import ModelTags
from opennode.oms.model.model.symlink import Symlink


class ITemplate(Interface):
    name = schema.TextLine(title=u"Template name", min_length=2)
    base_type = schema.Choice(title=u"Template type", values=(u'xen', u'kvm', u'openvz'))

    cores = schema.Tuple(
        title=u"Number of virtual cores", description=u"Minimum, suggested and maximum number of cores",
        value_type=schema.Int(),
        required=False)
    memory = schema.Tuple(
        title=u"Memory size", description=u"Minimum, suggested and maximum memory size (in GB)",
        value_type=schema.Float(),
        required=False)
    swap = schema.Tuple(
        title=u"Memory size", description=u"Minimum, suggested and maximum memory size (in GB)",
        value_type=schema.Float(),
        required=False)
    disk = schema.Tuple(
        title=u"Disk size", description=u"Minimum, suggested and maximum disk size",
        value_type=schema.Float(),
        required=False)
    cpu_limit = schema.Tuple(
        title=u"CPU usage limits", description=u"Minimum, suggested and maximum cpu_limit",
        value_type=schema.Int(),
        required=False)

    username = schema.TextLine(title=u"Default username", required=False)
    password = schema.TextLine(title=u"Default password", required=False)
    ip = schema.TextLine(title=u"Default IP", required=False)
    nameserver = schema.TextLine(title=u"Default nameserver", required=False)


class Template(Model):
    implements(ITemplate, IDisplayName)

    def __init__(self, name, base_type):
        self.name = name
        self.base_type = base_type

    def display_name(self):
        return self.name

    @property
    def nicknames(self):
        return [self.name, self.base_type]


class TemplateTags(ModelTags):
    context(Template)

    def auto_tags(self):
        return [u'virt_type:' + self.context.base_type]


class Templates(Container):
    __contains__ = Template
    __name__ = 'templates'

    def __init__(self, *args, **kw):
        super(Templates, self).__init__(*args, **kw)
        from opennode.knot.model.compute import IVirtualCompute
        self.__markers__ = [IVirtualCompute]

    def __str__(self):
        return 'Template list'


class GlobalTemplates(ReadonlyContainer):
    __contains__ = Template
    __name__ = 'templates'

    def __str__(self):
        return 'Global template list'

    @property
    def _items(self):
        # break an import cycle
        from opennode.oms.zodb import db
        machines = db.get_root()['oms_root']['machines']

        templates = {}

        def allowed_classes_gen(item):
            from opennode.knot.model.compute import ICompute, IVirtualCompute
            from opennode.knot.model.machines import Machines
            from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
            yield isinstance(item, Machines)
            yield isinstance(item, Templates)
            yield IVirtualizationContainer.providedBy(item)
            yield ICompute.providedBy(item)
            yield IVirtualCompute.providedBy(item)

        def collect(container):
            seen = set()
            for item in container.listcontent():
                if ITemplate.providedBy(item) and item.__name__ not in templates:
                    templates[item.__name__] = Symlink(item.__name__, item)

                if any(allowed_classes_gen(item)):
                    if item.__name__ not in seen:
                        seen.add(item.__name__)
                        collect(item)

        collect(machines)
        return templates


class TemplatesRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = GlobalTemplates


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(GlobalTemplates, ))
provideSubscriptionAdapter(ByNameContainerExtension, adapts=(Templates, ))
provideSubscriptionAdapter(ByNameContainerExtension, adapts=(GlobalTemplates, ))
