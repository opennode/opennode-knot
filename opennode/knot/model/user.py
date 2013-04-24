from __future__ import absolute_import

from grokcore.component import context, implements, name, GlobalUtility
from zope import schema
from zope.component import provideSubscriptionAdapter
from zope.interface import Interface

from opennode.knot.model.compute import IPreValidateHook

from opennode.oms.config import get_config
from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Model, Container
from opennode.oms.model.model.base import IMarkable, IDisplayName, ContainerInjector
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.model.traversal import traverse1
from opennode.oms.security.directives import permissions
from opennode.oms.zodb import db


class IUserProfile(Interface):

    name = schema.TextLine(title=u"User name", min_length=1)
    group = schema.TextLine(title=u"Group name", min_length=1)
    credit = schema.Int(title=u'User resource usage credit')


class UserProfile(Model):
    implements(IUserProfile, IDisplayName, IMarkable)
    permissions({'name': ('read', 'modify'),
                 'group': ('read', 'modify'),
                 'credit': ('read', 'modify')})

    __name__ = ''
    group = ''
    credit = 0

    def __init__(self, name, group, credit=0):
        self.__name__ = name
        self.group = group
        self.credit = credit

    def get_name(self):
        return self.__name__

    def set_name(self, value):
        self.__name__ = value

    name = property(get_name, set_name)

    def has_credit(self):
        ## TODO: make configurable
        return self.credit > 0

    def display_name(self):
        return self.name


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    name('user-credit-check')

    @db.ro_transact
    def check(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        if billable_group in map(str, principal.groups):
            profile = traverse1('/home/%s' % principal.id)
            assert profile is not None and profile.has_credit(), \
                    'User %s does not have enough credit' % principal.id


class IHome(Interface):
    pass


class Home(Container):
    implements(IHome)
    __contains__ = UserProfile
    __name__ = 'home'


class HomeRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Home


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(UserProfile, ))
