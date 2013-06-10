from grokcore.component import subscribe
from twisted.internet import defer
from twisted.internet import task
from twisted.internet import reactor
from twisted.python import log
from zope.authentication.interfaces import IAuthentication
from zope.component import handle
from zope.component import getUtility

import netaddr
import transaction

from opennode.knot.backend.compute import DeployAction, UndeployAction, DestroyComputeAction, AllocateAction
from opennode.knot.backend.operation import IResumeVM
from opennode.knot.backend.operation import IShutdownVM
from opennode.knot.backend.operation import IStartVM
from opennode.knot.backend.operation import ISuspendVM
from opennode.knot.backend.operation import IUpdateVM
from opennode.knot.backend.operation import ISetOwner
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.model.compute import ICompute, IVirtualCompute
from opennode.knot.model.compute import IDeployed
from opennode.knot.model.hangar import IHangar
from opennode.knot.model.user import IUserStatisticsProvider
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer

from opennode.oms.config import get_config
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.log import UserLogger
from opennode.oms.model.model.events import IModelModifiedEvent
from opennode.oms.model.model.events import IModelDeletedEvent
from opennode.oms.model.model.events import IModelCreatedEvent
from opennode.oms.model.model.events import IOwnerChangedEvent
from opennode.oms.model.model.events import ModelModifiedEvent
from opennode.oms.model.traversal import canonical_path, traverse1
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db


@subscribe(ICompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_compute_state_change_request(compute, event):

    if not event.modified.get('state', None):
        return

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
        return

    submitter = IVirtualizationContainerSubmitter(compute.__parent__)
    try:
        yield submitter.submit(action, compute.__name__)
    except Exception:
        compute.effective_state = event.original['state']
        raise
    else:
        compute.effective_state = event.modified['state']

    owner = (yield db.get(compute, '__owner__'))
    ulog = UserLogger()
    ulog.log('Changed state of %s (%s): %s -> %s' %
             (compute, (yield db.get(compute, '__owner__')), original, modified))
    yield defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner)

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

    owner = (yield db.get(model, '__owner__'))
    ulog = UserLogger(subject=model, owner=owner)
    ulog.log('Deleted %s' % model)
    yield defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner)

    @db.transact
    def deallocate_ip():
        ippools = db.get_root()['oms_root']['ippools']
        ip = netaddr.IPAddress(model.ipv4_address.split('/')[0])
        if ippools.free(ip):
            ulog.log('Deallocated IP: %s', ip)

    yield deallocate_ip()


def virtual_compute_action(action, path, event):

    @db.transact
    def run():
        model = traverse1(path)
        if model is None:
            log.msg('Model is not found while performing %s for %s: %s' % (action, event, path))
            return
        d = action(model).execute(DetachedProtocol(), object())
        d.addErrback(log.err)

    @db.data_integrity_validator
    def validate_db(r, compute):
        computes = db.get_root()['oms_root']['computes']
        db.log.debug('integrity: %s == %s', compute.__name__, list(computes._items))
        assert compute.__name__ in computes._items

    d = run()
    d.addCallback(validate_db)
    d.addErrback(log.err)


@subscribe(IVirtualCompute, IModelCreatedEvent)
def allocate_virtual_compute_from_hangar(model, event):
    if not IVirtualizationContainer.providedBy(model.__parent__):
        return

    if IDeployed.providedBy(model):
        return

    auto_allocate = get_config().getboolean('vms', 'auto_allocate', True)

    if not auto_allocate:
        return

    if IHangar.providedBy(model.__parent__.__parent__):
        action = AllocateAction
        msg = 'Allocated compute %s'
    elif ICompute.providedBy(model.__parent__.__parent__):
        action = DeployAction
        msg = 'Deployed compute %s'
    else:
        return

    try:
        path = canonical_path(model)
        owner = model.__owner__
        ul = UserLogger(subject=model, owner=owner)
        log.msg('Attempting %s for %s (%s, %s)' % (action.__name__, model, path, owner),
                system='create-event')
        d = task.deferLater(reactor, 2.0, virtual_compute_action, action, path, event)
        d.addCallback(lambda r: ul.log(msg % path))
        d.addCallback(lambda r: defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner))
        d.addErrback(log.err)
    except Exception:
        log.err(system='create-event')


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
    except Exception as e:
        @db.transact
        def reset_to_original_values():
            for mk, mv in event.modified.iteritems():
                setattr(compute, mk, event.original[mk])
        yield reset_to_original_values()
        raise e  # must re-throw, because sys.exc_info seems to get erased with the yield
    else:
        owner = (yield db.get(compute, '__owner__'))
        UserLogger(subject=compute, owner=owner).log('Compute "%s" configuration changed' % compute)
        yield defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner)


@subscribe(IVirtualCompute, IOwnerChangedEvent)
def handle_ownership_change(model, event):

    @defer.inlineCallbacks
    def update_statistics_after_commit(oldowner, newowner):
        log.msg('%s owner changed from %s to %s: updating user statistics' %
                (model, oldowner, newowner), system='ownership-change-event')
        try:
            for owner in (newowner, oldowner):
                yield defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner)
        except Exception:
            log.err(system='ownership-change-event')
            raise

    def update_statistics_dbhook(success, *args):
        if success:
            blocking_yield(update_statistics_after_commit(*args))

    msg = 'Compute "%s" owner changed from "%s" to "%s"' % (model, event.oldowner, event.nextoner)

    oldowner = getUtility(IAuthentication).getPrincipal(event.oldowner)
    newowner = getUtility(IAuthentication).getPrincipal(event.nextowner)
    oldulog = UserLogger(subject=model, owner=oldowner)
    newulog = UserLogger(subject=model, owner=newowner)
    log.msg(msg, system='ownership-change-event')
    oldulog.log(msg)
    newulog.log(msg)

    try:
        submitter = IVirtualizationContainerSubmitter(model.__parent__)
        blocking_yield(submitter.submit(ISetOwner, model.__name__, model.__owner__))
    except Exception:
        log.err(system='ownership-change-event')
        raise

    curtransaction = transaction.get()
    curtransaction.addAfterCommitHook(update_statistics_dbhook, args=(event.oldowner, event.nextowner))
