from grokcore.component import implements, GlobalUtility
from StringIO import StringIO
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent, ResponseDone
from twisted.web.http_headers import Headers
from twisted.web.http import PotentialDataLoss
from twisted.web.iweb import IBodyProducer
from urllib import urlencode

import json
import hashlib
import logging
import sys

from opennode.knot.backend.billing import ICreditCheckCall

from opennode.oms.config import get_config


log = logging.getLogger(__name__)


class ResponseProtocol(Protocol):
    def __init__(self, finished, size):
        self.finished = finished
        self.remaining = size
        self.data = StringIO()

    def dataReceived(self, bytes):
        display = bytes[:self.remaining]
        self.data.write(display)
        self.remaining -= len(display)

    def connectionLost(self, reason):
        if type(reason.value) in (ResponseDone, PotentialDataLoss):
            self.finished.callback(self.data.getvalue())
        else:
            self.finished.errback(reason)


class WHMCSAPIError(Exception):
    pass


class WHMCSRequestBody(object):
    implements(IBodyProducer)

    def __init__(self, data):
        self.data = urlencode(data)
        self.length = len(self.data)

    def startProducing(self, consumer):
        consumer.write(self.data)
        return defer.succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class WhmcsCreditChecker(GlobalUtility):
    implements(ICreditCheckCall)

    @defer.inlineCallbacks
    def get_credit(self, uid):
        log.info('Requesting credit update for %s', uid)
        try:
            agent = Agent(reactor)
            whmcs_api_uri = get_config().getstring('whmcs', 'api_uri')
            whmcs_user = get_config().getstring('whmcs', 'user', '')
            whmcs_password = get_config().getstring('whmcs', 'password', '')

            pwmd5 = hashlib.md5()
            pwmd5.update(whmcs_password)
            reqbody = WHMCSRequestBody({'username': whmcs_user,
                                        'password': pwmd5.hexdigest(),
                                        'clientid': uid,
                                        'action': 'getclientsdetails',
                                        'responsetype': 'json'})

            headers = Headers({'User-Agent': ['OMS-KNOT 2.0'],
                               'Content-Type': ['application/x-www-form-urlencoded']})

            response = yield agent.request('POST', whmcs_api_uri, headers, reqbody)

            finished = defer.Deferred()
            rbody = ResponseProtocol(finished, 1024 * 10)
            response.deliverBody(rbody)
            data = yield finished
        except Exception as e:
            log.error(e, exc_info=sys.exc_info())
            raise

        if response.code < 400:
            data = json.loads(data)
            if data.get('result') == 'error':
                raise WHMCSAPIError('%s' % (data.get('message')))
            credit_balance_field = 'customfields%s' % get_config().getstring('whmcs', 'balance_limit', 2)
            credit_balance = float(data.get(credit_balance_field)) if credit_balance_field in data else 0
            defer.returnValue(float(data.get('credit')), credit_balance)

        raise WHMCSAPIError('%s: %s' % (response.code, data))
