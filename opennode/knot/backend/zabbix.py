from __future__ import absolute_import

from grokcore.component import subscribe
from opennode.oms.model.form import IModelCreatedEvent, IModelDeletedEvent
from opennode.knot.model.compute import ICompute
from opennode.knot.model.hangar import IHangar


@subscribe(ICompute, IModelCreatedEvent)
def add_compute_to_zabbix(model, event):
    if IHangar.providedBy(model.__parent__):
        return
    # FILL IT
    print "TODO: add %s to zabbix" % (model,)


@subscribe(ICompute, IModelDeletedEvent)
def remove_compute_from_zabbix(model, event):
    if IHangar.providedBy(model.__parent__):
        return
    # FILL IT
    # exception thrown will prevent deletion
    print "TODO: remove %s from zabbix" % (model,)
