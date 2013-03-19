from __future__ import absolute_import

from zope.interface import Interface, implements

from opennode.oms.model.model.base import Container
from opennode.oms.security.directives import permissions


class IHangar(Interface):
    pass


class IInHangar(Interface):
    """ Analogous to IInCompute for items allowed to be contained in a Hangar """
    pass


class Hangar(Container):
    implements(IHangar)
    permissions(dict(backend=('read', 'modify')))

    __contains__ = IInHangar
    __name__ = 'hangar'

    def __str__(self):
        return self.__name__
