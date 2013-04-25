from __future__ import absolute_import

from grokcore.component import context, implements
from zope import schema
from zope.component import provideSubscriptionAdapter
from zope.interface import Interface


from opennode.oms.model.model.actions import ActionsContainerExtension
from opennode.oms.model.model.base import Model, Container
from opennode.oms.model.model.base import IMarkable, IDisplayName, ContainerInjector
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.security.directives import permissions


class IUserProfile(Interface):

    name = schema.TextLine(title=u"User name", min_length=1)
    group = schema.TextLine(title=u"Group name", min_length=1)
    credit = schema.Int(title=u'User resource usage credit')
    credit_timestamp = schema.TextLine(title=u'Timestamp', description=u'Timestamp of credit recording')


class UserProfile(Model):
    implements(IUserProfile, IDisplayName, IMarkable)
    permissions({'name': ('read', 'modify'),
                 'group': ('read', 'modify'),
                 'credit': ('read', 'modify')})

    __name__ = ''
    group = ''
    _credit = 0
    _credit_timestamp = ''

    def __init__(self, name, group, credit=0, credit_timestamp=''):
        self.__name__ = name
        self.group = group
        self.credit = credit
        self.credit_timestamp = credit_timestamp

    def get_name(self):
        return self.__name__

    def set_name(self, value):
        self.__name__ = value

    name = property(get_name, set_name)

    @property
    def credit_timestamp(self):
        return self._credit_timestamp

    def set_credit(self, value):
        self._credit = value

    def get_credit(self):
        return self._credit

    credit = property(get_credit, set_credit)

    def has_credit(self):
        ## TODO: make configurable
        return self.credit > 0

    def display_name(self):
        return self.name


class IHome(Interface):
    """ User profile container """
    pass


class Home(Container):
    implements(IHome)
    __contains__ = UserProfile
    __name__ = 'home'


class HomeRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Home


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(UserProfile, ))
