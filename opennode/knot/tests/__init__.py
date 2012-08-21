from opennode.oms.zodb.db import init
from opennode.oms.core import setup_environ


def setup_package():
    init(test=True)
    setup_environ(test=True)


def teardown_package():
    from opennode.oms.tests.util import teardown_reactor

    teardown_reactor()
