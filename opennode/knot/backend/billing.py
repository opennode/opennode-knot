from grokcore.component import context, implements, name, GlobalUtility
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent, ResponseDone
from twisted.web.http_headers import Headers
from twisted.web.http import PotentialDataLoss
from twisted.web.iweb import IBodyProducer
from zope.interface import Interface

import json
from urllib import urlencode

from opennode.knot.model.compute import IVirtualCompute

from opennode.oms.config import get_config
from opennode.oms.model.model.hooks import IPreValidateHook
from opennode.oms.model.traversal import traverse1
from opennode.oms.zodb import db


class UserCreditChecker(GlobalUtility):
    implements(IPreValidateHook)
    context(IVirtualCompute)
    name('user-credit-check')

    @db.ro_transact
    def apply(self, principal):
        billable_group = get_config().getstring('auth', 'billable_group', 'users')
        if billable_group in map(str, principal.groups):
            profile = traverse1('/home/%s' % principal.id)
            assert profile is not None and profile.has_credit(), \
                    'User %s does not have enough credit' % principal.id

    def applicable(self, context):
        if IVirtualCompute.providedBy(context):
            return True
        return False


class ICreditCheckCall(Interface):
    def get_credit(self, uid):
        """ Get credit """

class ResponseProtocol(Protocol):
    def __init__(self, finished, size):
        self.finished = finished
        self.remaining = size

    def dataReceived(self, bytes):
        display = bytes[:self.remaining]
        print display
        self.remaining -= len(display)

    def connectionLost(self, reason):
        print 'Finished receiving body:', reason.getErrorMessage()
        if type(reason.value) in (ResponseDone, PotentialDataLoss):
            self.finished.callback(self.data)
        else:
            self.finished.errback(reason)


class WHMCSRequestBody(object):
    implements(IBodyProducer)

    def __init__(self, data):
        self.data = data

    def startProducing(self, consumer):
        consumer.write('&'.join(['%s=%s' % (key, val) for key, val in self.data.iteritems()]))
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class WHMCSCreditChecker(GlobalUtility):
    implements(ICreditCheckCall)
    name('user-credit-check-whmcs')

    @defer.inlineCallbacks
    def get_credit(self, uid):
        agent = Agent(reactor)
        whmcs_user = get_config().getstring('whmcs', 'user')
        whmcs_password = get_config().getstring('whmcs', 'password')

        reqbody = WHMCSRequestBody({'user': urlencode(whmcs_user),
                                    'password': urlencode(whmcs_password),
                                    'clientid': uid,
                                    'action': 'getclientsdetails',
                                    'responsetype': 'json'})

        response = yield agent.request('POST', Headers({'User-Agent': ['OMS-KNOT 2.0']}), reqbody)

        finished = defer.Deferred()
        rbody = ResponseProtocol(finished, response.headers['Content-Length'])
        response.deliverBody(rbody)

        if response.code < 400:
            data = yield finished
            data = json.loads(data)
            defer.returnValue(data.get('credit'))

        raise Exception('Error checking credit: %s: %s' % (response.code, data))
