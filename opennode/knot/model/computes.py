from __future__ import absolute_import

from grokcore.component import context
import logging
from types import GeneratorType
from twisted.python import log
from zope.component import provideSubscriptionAdapter

from opennode.knot.model.compute import Compute, ICompute, IVirtualCompute
from opennode.knot.model.hangar import IHangar

from opennode.oms.model.model.base import AddingContainer, ReadonlyContainer
from opennode.oms.model.model.base import ContainerInjector
from opennode.oms.model.model.byname import ByNameContainerExtension
from opennode.oms.model.model.proc import ITask
from opennode.oms.model.model.root import OmsRoot
from opennode.oms.model.model.symlink import Symlink
from opennode.oms.zodb import db


class Computes(AddingContainer):
    __contains__ = IVirtualCompute
    __name__ = 'computes'

    def __str__(self):
        return 'Compute list'

    @property
    def _items(self):
        machines = db.get_root()['oms_root']['machines']

        computes = {}

        def allowed_classes_gen(item):
            from opennode.knot.model.machines import Machines
            from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
            yield isinstance(item, Machines)
            yield isinstance(item, Computes)
            yield ICompute.providedBy(item)
            yield IVirtualizationContainer.providedBy(item)
            yield IHangar.providedBy(item)

        def collect(container):
            seen = set()
            for item in container.listcontent():
                if ICompute.providedBy(item):
                    computes[item.__name__] = Symlink(item.__name__, item)

                if any(allowed_classes_gen(item)):
                    if item.__name__ not in seen:
                        seen.add(item.__name__)
                        collect(item)

        collect(machines)
        return computes

    def _add(self, item):
        machines = db.get_root()['oms_root']['machines']
        # TODO: fix adding computes to vms instead of hangar
        if not machines.hangar['vms']:
            pass
        return (machines.hangar if IVirtualCompute.providedBy(item) else machines).add(item)

    def __delitem__(self, key):
        item = self._items[key]
        if isinstance(item, Symlink):
            del item.target.__parent__[item.target.__name__]


class ComputesRootInjector(ContainerInjector):
    context(OmsRoot)
    __class__ = Computes


class ComputeTasks(ReadonlyContainer):
    context(Compute)
    __contains__ = ITask
    __name__ = 'tasks'

    @property
    def _items(self):
        from opennode.oms.zodb import db
        processes = db.get_root()['oms_root']['proc']
        tasks = {}

        def collect(container):
            seen = set()
            for item in container.listcontent():
                name = item.__name__
                if not ITask.providedBy(item):
                    continue

                # Cmd.subject() implemented incorrectly (must not return a generator)
                # XXX: for some reason, when I let subject stick to a generator instance,
                # I get an empty generator here, while it magically works when I save
                # it as a tuple under item.subject
                assert not isinstance(item.subject, GeneratorType)

                iterable = isinstance(item.subject, tuple)

                if iterable:
                    log.msg('\t CMD: %s %s' % (item.__name__, item.cmdline),
                            system='proc',
                            logLevel=logging.DEBUG)
                    for s in item.subject:
                        log.msg('\t\tSUBJ: %s, %s' % (s.__name__ == self.__parent__.__name__,
                                                      self.__parent__),
                                system='proc',
                                logLevel=logging.DEBUG)

                if iterable and self.__parent__.__name__ in map(lambda s: s.__name__, item.subject):
                    tasks[name] = Symlink(name, item)

                if name not in seen:
                    seen.add(name)
                    collect(item)

        collect(processes)
        return tasks


class ComputeTasksInjector(ContainerInjector):
    context(Compute)
    __class__ = ComputeTasks


provideSubscriptionAdapter(ByNameContainerExtension, adapts=(Computes, ))
