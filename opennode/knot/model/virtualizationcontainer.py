from __future__ import absolute_import

from grokcore.component import context
from zope import schema
from zope.component import provideSubscriptionAdapter
from zope.interface import Interface, implements

from opennode.knot.model.compute import IVirtualCompute, IInCompute
from opennode.knot.model.hangar import IInHangar
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Container
from opennode.oms.model.model.base import ContainerInjector
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.oms.model.model.search import ModelTags
from opennode.oms.model.model.proc import Proc
from opennode.oms.security.directives import permissions


class IVirtualizationContainer(Interface):
    backend = schema.Choice(title=u"Backend", values=(u'xen', u'kvm', u'openvz', u'lxc'))


class VirtualizationContainer(Container):
    implements(IVirtualizationContainer, IInCompute, IInHangar)
    permissions(dict(backend=('read', 'modify')))

    __contains__ = IVirtualCompute

    def __init__(self, backend):
        super(VirtualizationContainer, self).__init__()
        self.backend = backend
        self.__name__ = 'vms-%s' % backend

    def __str__(self):
        return 'virtualizationcontainer-%s' % self.__name__


class VirtualizationContainerTags(ModelTags):
    context(VirtualizationContainer)

    def auto_tags(self):
        return [u'virt:' + self.context.backend]


class IGlobalIndentifierProvider(Interface):
    identifier = schema.Int(title=u'Current identifier value', default=101)


class GlobalIdentifierProvider(object):
    implements(IGlobalIndentifierProvider)
    permissions(dict(backend=('read', 'modify')))

    _id = None

    def get_ident(self):
        return self._id

    def set_ident(self, value):
        self._id = value

    ident = property(get_ident, set_ident)


class GlobalIdentifierProviderInjector(ContainerInjector):
    context(Proc)
    __class__ = GlobalIdentifierProvider


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(VirtualizationContainer, ))
provideSubscriptionAdapter(ByNameContainerExtension, adapts=(VirtualizationContainer, ))
