from __future__ import absolute_import

from twisted.python import log

from grokcore.component import subscribe
from opennode.oms.model.form import IModelCreatedEvent, IModelDeletedEvent
from opennode.knot.model.compute import ICompute
from opennode.knot.model.hangar import IHangar


@subscribe(ICompute, IModelCreatedEvent)
def add_compute_to_zabbix(model, event):
    if IHangar.providedBy(model.__parent__):
        return
    # FILL IT
    log.msg("TODO: add %s to zabbix" % (model,), system='zabbix')


@subscribe(ICompute, IModelDeletedEvent)
def remove_compute_from_zabbix(model, event):
    if IHangar.providedBy(model.__parent__):
        return
    # FILL IT
    # exception thrown will prevent deletion
    log.msg("TODO: remove %s from zabbix" % (model,), system='zabbix')
