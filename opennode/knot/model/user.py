from __future__ import absolute_import

from datetime import datetime
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
    uid = schema.Int(title=u'ID', description=u'Application-specific numerical ID', required=False)
    name = schema.TextLine(title=u"Name", min_length=1)
    groups = schema.List(title=u"Groups", min_length=1)
    credit = schema.Int(title=u'Resource usage credit', required=False)
    credit_timestamp = schema.TextLine(title=u'Last credit update',
                                       description=u'Timestamp of last credit recording',
                                       required=False)


class UserProfile(Model):
    implements(IUserProfile, IDisplayName, IMarkable)
    permissions({'name': ('read', 'modify'),
                 'group': ('read', 'modify'),
                 'credit': ('read', 'modify'),
                 'userid': ('read', 'modify')})

    __name__ = None
    groups = []
    _credit = 0
    _credit_timestamp = None
    uid = None

    def __init__(self, name, groups, credit=0, credit_timestamp='', uid=0):
        self.__name__ = name
        self.groups = groups
        self.credit = credit
        self._credit_timestamp = credit_timestamp if credit_timestamp else self._credit_timestamp
        self.uid = uid

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
        self._credit_timestamp = datetime.now().isoformat()

    def get_credit(self):
        return self._credit

    credit = property(get_credit, set_credit)

    def has_credit(self):
        ## TODO: make configurable
        return self.credit > 0

    def display_name(self):
        return self.name

    def __repr__(self):
        return "UserProfile('%s', %s, %s, '%s', %s)" % (self.__name__, self.groups, self.credit,
                                                        self.credit_timestamp, self.uid)

class IHome(Interface):
    """ User profile container """
    pass


class Home(Container):
    implements(IHome)
    __contains__ = UserProfile
    __name__ = 'home'

    def _new_id(self):
        raise TypeError('This container does not support generated IDs')

    def content(self):
        return super(Home, self).content()


class HomeRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Home


provideSubscriptionAdapter(ActionsContainerExtension, adapts=(UserProfile, ))
