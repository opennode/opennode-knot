import os
import shutil

from xml.etree import ElementTree

from twisted.internet import defer
from zope.interface import alsoProvides

import opennode.knot.tests
from opennode.knot.backend.operation import IGetComputeInfo, IStartVM, IShutdownVM, IListVMS, IUpdateVM
from opennode.oms.model.form import RawDataApplier
from opennode.knot.model.compute import Compute
from opennode.knot.model.compute import ISaltInstalled
from opennode.knot.model.virtualizationcontainer import VirtualizationContainer
from opennode.oms.tests.util import run_in_reactor


def make_compute(hostname=u'tux-for-test', state=u'active', memory=2000):
    return Compute(hostname, state, memory)


@run_in_reactor
def test_get_info():
    compute = make_compute(hostname=u'localhost')
    alsoProvides(compute, ISaltInstalled)
    res = yield IGetComputeInfo(compute, None).run()
    print IGetComputeInfo(compute, None)
    assert res[1]['name'] == 'localhost'


@run_in_reactor
@defer.inlineCallbacks
def test_operate_vm():
    compute = make_compute(hostname=u'localhost')
    alsoProvides(compute, ISaltInstalled)

    backend = 'test://' + os.path.join(opennode.knot.tests.__path__[0], "u1.xml")

    job = IStartVM(compute)
    res = yield job.run(backend, '4dea22b31d52d8f32516782e98ab3fa0')
    assert res is None

    job = IShutdownVM(compute)
    res = yield job.run(backend, 'EF86180145B911CB88E3AFBFE5370493')
    assert res is None

    job = IListVMS(compute)
    res = yield job.run(backend)
    assert res[1]['name'] == 'vm1'
    assert res[1]['state'] == 'inactive'


@run_in_reactor
def test_activate_compute():
    shutil.copy(os.path.join(opennode.knot.tests.__path__[0], 'u1.xml'), '/tmp/salt_vm_test_state.xml')

    compute = make_compute(hostname=u'vm1', state=u'inactive')
    compute.__name__ = '4dea22b31d52d8f32516782e98ab3fa0'

    dom0 = make_compute(hostname=u'localhost', state=u'active')
    dom0.__name__ = 'f907e3553a8c4cc5a6db1790b65f93f8'
    alsoProvides(dom0, ISaltInstalled)

    container = VirtualizationContainer('test')
    container.__parent__ = dom0
    compute.__parent__ = container

    assert compute.effective_state == 'inactive'
    # force effective state because it's a lazy attribute
    compute.effective_state = u'inactive'

    RawDataApplier({'state': u'active'}, compute).apply()

    root = ElementTree.parse('/tmp/salt_vm_test_state.xml')
    for node in root.findall('domain'):
        if node.find('name').text == 'vm1':
            assert node.attrib['state'] == 'inactive'
            return
    assert False


@run_in_reactor
@defer.inlineCallbacks
def test_update_vm():
    compute = make_compute(hostname=u'localhost')
    alsoProvides(compute, ISaltInstalled)

    backend = 'test://' + os.path.join(os.getcwd(), "opennode/knot/tests/u1.xml")

    job = IListVMS(compute)
    res = yield job.run(backend)
    assert res[1]['name'] == 'vm1'
    assert res[1]['state'] == 'inactive'

    job = IUpdateVM(compute)
    res = yield job.run(backend)
    assert res is None
