from __future__ import absolute_import

from grokcore.component import context
import logging
from types import GeneratorType
from twisted.python import log
from zope import schema
from zope.component import provideSubscriptionAdapter, provideAdapter
from zope.interface import Interface, implements

from opennode.oms.config import get_config
from opennode.oms.model.form import alsoProvides
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Model, Container
from opennode.oms.model.model.base import IMarkable, IDisplayName, ContainerInjector
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.security.directives import permissions
from opennode.oms.util import adapter_value


class IUserProfile(Interface):

    name = schema.TextLine(title=u"User name", min_length=1)
    group = schema.TextLine(title=u"Group name", min_length=1)
    credit = schema.Int(title=u'User resource usage credit')


class UserProfile(Model):
    implements(IUserProfile, IDisplayName, IMarkable)
    permissions({'name': ('read', 'modify'),
                 'group': ('read', 'modify'),
                 'credit': ('read', 'modify')})

    name = ''
    group = ''
    credit = 0

    def __init__(self, name, group, credit=0):
        self.name = name
        self.group = group
        self.credit = credit


    def has_credit(self):
        ## TODO: make configurable
        return self.credit > 0


    def display_name(self):
        return self.name


class IHome(Interface):
    pass


class Home(Container):
    implements(IHome)
    __contains__ = UserProfile
    context(OmsRoot)


class HomeRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Home


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(UserProfile, ))
