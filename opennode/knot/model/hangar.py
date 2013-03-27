from __future__ import absolute_import

from grokcore.component import context
from zope.interface import Interface, implements

from opennode.oms.model.model.base import Container, ContainerInjector


class IHangar(Interface):
    pass


class IInHangar(Interface):
    """ Analogous to IInCompute for items allowed to be contained in a Hangar """
    pass


class Hangar(Container):
    implements(IHangar)

    __contains__ = IInHangar
    __name__ = 'hangar'

    def __str__(self):
        return self.__name__

