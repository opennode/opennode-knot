"""
Connections to the back-ends, managed by OMS.
"""
from grokcore.component import context, Adapter
from zope.interface import implements

from opennode.knot.model.compute import ICompute
from opennode.knot.backend.operation import IMinion
from opennode.oms.zodb import db
from opennode.oms.security.authentication import Sudo


class ComputeMinion(Adapter):
    implements(IMinion)
    context(ICompute)

    @db.ro_transact
    def hostname(self):
        with Sudo(self.context):
            return self.context.hostname
