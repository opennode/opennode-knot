from grokcore.component import subscribe
from twisted.internet import defer
from twisted.python import log
from zope.component import handle

import netaddr

from opennode.knot.backend.compute import DeployAction, UndeployAction, DestroyComputeAction, AllocateAction
from opennode.knot.backend.operation import IResumeVM
from opennode.knot.backend.operation import IShutdownVM
from opennode.knot.backend.operation import IStartVM
from opennode.knot.backend.operation import ISuspendVM
from opennode.knot.backend.operation import IUpdateVM
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.model.compute import ICompute, IVirtualCompute
from opennode.knot.model.compute import IDeployed
from opennode.knot.model.hangar import IHangar
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer

from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.log import UserLogger
from opennode.oms.model.form import IModelModifiedEvent
from opennode.oms.model.form import IModelDeletedEvent
from opennode.oms.model.form import IModelCreatedEvent
from opennode.oms.model.form import ModelModifiedEvent
from opennode.oms.util import exception_logger
from opennode.oms.zodb import db


@subscribe(ICompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_compute_state_change_request(compute, event):

    if not event.modified.get('state', None):
        defer.returnValue(None)

    def get_action(original, modified):
        action_mapping = {'inactive': {'active': IStartVM},
                          'suspended': {'active': IResumeVM},
                          'active': {'inactive': IShutdownVM,
                                     'suspended': ISuspendVM}}

        action = action_mapping.get(original, {}).get(modified, None)
        return action

    original = event.original['state']
    modified = event.modified['state']
    action = get_action(original, modified)

    if not action:
        defer.returnValue(None)

    submitter = IVirtualizationContainerSubmitter(compute.__parent__)
    try:
        yield submitter.submit(action, compute.__name__)
    except Exception:
        compute.effective_state = event.original['state']
        raise
    else:
        compute.effective_state = event.modified['state']

    ulog = UserLogger()
    ulog.log('Changed state of %s (%s): %s -> %s',
             compute, (yield db.get(compute, '__owner__')), original, modified)

    handle(compute, ModelModifiedEvent({'effective_state': event.original['state']},
                                       {'effective_state': compute.effective_state}))


@subscribe(IVirtualCompute, IModelDeletedEvent)
@defer.inlineCallbacks
def delete_virtual_compute(model, event):
    if not ICompute.providedBy(model.__parent__.__parent__):
        return

    if IDeployed.providedBy(model):
        log.msg('Deleting compute %s which is in IDeployed state, shutting down and '
                'undeploying first' % model.hostname, system='compute-backend')
        yield DestroyComputeAction(model).execute(DetachedProtocol(), object())
        yield UndeployAction(model).execute(DetachedProtocol(), object())
    else:
        log.msg('Deleting compute %s which is already in IUndeployed state' %
                model.hostname, system='compute-backend')

    ulog = UserLogger(subject=model, owner=(yield db.get(model, '__owner__')))
    ulog.log('Deleted compute')

    @db.transact
    def deallocate_ip():
        ippools = db.get_root()['oms_root']['ippools']
        ip = netaddr.IPAddress(model.ipv4_address.split('/')[0])
        if ippools.free(ip):
            ulog.log('Deallocated IP: %s', ip)

    yield deallocate_ip()


@subscribe(IVirtualCompute, IModelCreatedEvent)
@defer.inlineCallbacks
def create_virtual_compute(model, event):
    if not IVirtualizationContainer.providedBy(model.__parent__):
        return

    if not ICompute.providedBy(model.__parent__.__parent__):
        return

    if IDeployed.providedBy(model):
        return

    log.msg('Deploying VM "%s"' % model, system='deploy')
    yield exception_logger(DeployAction(model)._execute)(DetachedProtocol(), object())

    UserLogger(subject=model, owner=(yield db.get(model, '__owner__'))).log('Deployed compute %s' % model)


@subscribe(IVirtualCompute, IModelCreatedEvent)
@defer.inlineCallbacks
def allocate_virtual_compute_from_hangar(model, event):
    if not IVirtualizationContainer.providedBy(model.__parent__):
        return

    if not IHangar.providedBy(model.__parent__.__parent__):
        return

    if IDeployed.providedBy(model):
        return

    log.msg('Auto-allocating VM "%s"' % model, system='allocate')
    yield exception_logger(AllocateAction(model)._execute)(DetachedProtocol(), object())

    UserLogger(subject=model, owner=(yield db.get(model, '__owner__'))).log('Allocated compute %s' % model)


@subscribe(IVirtualCompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_virtual_compute_config_change_request(compute, event):
    update_param_whitelist = ['cpu_limit',
                              'memory',
                              'num_cores',
                              'swap_size']

    params_to_update = filter(lambda (k, v): k in update_param_whitelist, event.modified.iteritems())

    if len(params_to_update) == 0:
        return

    update_values = [v for k, v in sorted(params_to_update, key=lambda (k, v): k)]

    submitter = IVirtualizationContainerSubmitter((yield db.get(compute, '__parent__')))
    try:
        yield submitter.submit(IUpdateVM, (yield db.get(compute, '__name__')), *update_values)
    except Exception:
        for mk, mv in event.modified.iteritems():
            setattr(compute, mk, event.original[mk])
        raise
    else:
        ulog = UserLogger(subject=compute, owner=(yield db.get(compute, '__owner__')))
        ulog.log('Compute configuration changed')
