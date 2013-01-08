from __future__ import absolute_import

from zope.interface import Interface, implements

from opennode.oms.model.model.base import Container
from opennode.knot.model.compute import IVirtualCompute, IInCompute


class IHangar(Interface):
    pass


class Hangar(Container):
    implements(IHangar, IInCompute)

    __contains__ = IVirtualCompute
    __name__ = 'hangar'

    def __str__(self):
        return self.__name__
