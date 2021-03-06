import logging

from grokcore.component import subscribe
from twisted.internet import defer
from twisted.internet import task
from twisted.internet import reactor
from twisted.python import log
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility

import netaddr
import transaction

from opennode.knot.backend.compute import DeployAction, UndeployAction, DestroyComputeAction, AllocateAction
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
from opennode.oms.model.traversal import canonical_path, traverse1
from opennode.oms.security.authentication import sudo
from opennode.oms.util import blocking_yield
from opennode.oms.zodb import db

logger = logging.getLogger(__name__)


@defer.inlineCallbacks
def update_statistics_after_commit(owners):
    log.msg('Statistics event handler for %s: updating user statistics' % (owners), system='event')
    try:
        for owner in owners:
            yield defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner)
    except Exception:
        log.err(system='event')
        raise


def update_statistics_dbhook(success, *args):
    if success and not get_config().getboolean('stats', 'only_report_on_sync', True):
        blocking_yield(update_statistics_after_commit(*args))


@subscribe(ICompute, IModelModifiedEvent)
def handle_compute_state_change_request(compute, event):

    if not event.modified.get('state', None):
        return

    original = event.original['state']
    modified = event.modified['state']

    if original == modified:
        return

    owner = compute.__owner__

    ulog = UserLogger()
    ulog.log('Changed state of %s (%s): %s -> %s' % (compute, owner, original, modified))
    log.msg('Changed state of %s (%s): %s -> %s' % (compute, owner, original, modified),
            system='state-change')

    curtransaction = transaction.get()
    curtransaction.addAfterCommitHook(update_statistics_dbhook, args=([owner],))


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

    @db.transact
    def deallocate_ip():
        ippools = db.get_root()['oms_root']['ippools']
        ip = netaddr.IPAddress(model.ipv4_address.split('/')[0])
        if ippools.free(ip):
            ulog.log('Deallocated IP: %s', ip)

    yield deallocate_ip()


@subscribe(IVirtualCompute, IModelDeletedEvent)
def delete_virtual_compute_update_stats(model, event):
    owner = model.__owner__
    curtransaction = transaction.get()
    curtransaction.addAfterCommitHook(update_statistics_dbhook, args=([owner],))


def virtual_compute_action(action, path, event):

    @db.transact
    def run():
        model = traverse1(path)
        if model is None:
            log.msg('Model is not found while performing %s for %s: %s' % (action, event, path))
            return
        d = action(model).execute(DetachedProtocol(), object())
        d.addErrback(log.err)

    d = run()
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
        if not get_config().getboolean('stats', 'only_report_on_sync', True):
            d.addCallback(lambda r: defer.maybeDeferred(getUtility(IUserStatisticsProvider).update, owner))
        d.addErrback(log.err)
    except Exception:
        log.err(system='create-event')


@subscribe(IVirtualCompute, IModelModifiedEvent)
@defer.inlineCallbacks
def handle_virtual_compute_config_change_request(compute, event):
    c = sudo(compute)
    compute_p = yield db.get(c, '__parent__')
    compute_type = yield db.get(compute_p, 'backend')
    # At the moment we only handle openvz backend updates (OMS-568)
    if compute_type != 'openvz':
        return

    update_param_whitelist = ['diskspace',
                              'memory',
                              'num_cores',
                              'swap_size']

    param_modifier = {'diskspace': lambda d: d['total']}

    unit_corrections_coeff = {'memory': 1 / 1024.0,
                              'swap_size': 1 / 1024.0,
                              'diskspace': 1 / 1024.0}

    params_to_update = filter(lambda (k, v): k in update_param_whitelist, event.modified.iteritems())

    if len(params_to_update) == 0:
        return

    # correct unit coefficients (usually MB -> GB)
    params_to_update = map(lambda (k, v): (k, param_modifier.get(k, lambda x: x)(v)), params_to_update)
    params_to_update = map(lambda (k, v): (k, unit_corrections_coeff.get(k) * v
                                            if k in unit_corrections_coeff else v), params_to_update)

    @db.transact
    def update_vm_limits(cpu_limit):
        logger.debug("Setting cpu_limit to %s, previous value %s" % (cpu_limit / 100.0, c.cpu_limit))
        c.cpu_limit = cpu_limit / 100.0
    
    cores_setting = filter(lambda(k, v): k == 'num_cores', params_to_update)
    if len(cores_setting) == 1:
        # adjust cpu_limit to follow the number of cores as well
        cpu_limit = int(cores_setting[0][1] * get_config().getfloat('vms', 'cpu_limit', 80))
        log.msg("Updating cpulimit to %s" % cpu_limit, system='vm-configuration-update')

        params_to_update.append(('cpu_limit', cpu_limit))
        yield update_vm_limits(cpu_limit)

    submitter = IVirtualizationContainerSubmitter((yield db.get(compute, '__parent__')))
    try:
        yield submitter.submit(IUpdateVM, (yield db.get(compute, '__name__')), dict(params_to_update))
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


@subscribe(IVirtualCompute, IModelModifiedEvent)
def handle_config_change_update_stats(compute, event):
    update_param_whitelist = ['cpu_limit',
                              'memory',
                              'num_cores',
                              'swap_size']

    params_to_update = filter(lambda (k, v): k in update_param_whitelist, event.modified.iteritems())

    if len(params_to_update) == 0:
        return

    owner = compute.__owner__
    curtransaction = transaction.get()
    curtransaction.addAfterCommitHook(update_statistics_dbhook, args=([owner],))


@subscribe(IVirtualCompute, IOwnerChangedEvent)
def handle_ownership_change(model, event):
    if model.__parent__ is None:
        log.msg('Ownership change event handler suppressed: model is not attached to a parent yet',
                system='ownership-change-event')
        return

    msg = 'Compute "%s" owner changed from "%s" to "%s"' % (model, event.oldowner, event.nextowner)
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
    curtransaction.addAfterCommitHook(update_statistics_dbhook, args=([event.oldowner, event.nextowner],))
