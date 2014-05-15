# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

import time

from urllib import urlencode

from twisted.internet import defer, reactor
from twisted.web.client import Agent

from ..backend.appl import JSONReceiver, PBReceiver

class ApplInfo(object):
    reactor = reactor
    def __init__(self, infourl):
        self.agent = Agent(self.reactor)
        self.infourl = infourl
        self._info = self._info_time = None

    @defer.inlineCallbacks
    def getInfo(self):
        if self._info is not None and time.time()-self._info_time<3600:
            defer.returnValue(self._info)

        R = yield self.agent.request('GET', self.infourl)
        if R.code==404:
            raise RuntimeError('Not an Archiver Appliance')
        elif R.code!=200:
            raise RuntimeError('%d: %s'%(R.code, self.infourl))

        J = JSONReceiver()
        R.deliverBody(J)
        self._info = D = yield J.defer
        self._info_time = time.time()

        defer.returnValue(D)

    @defer.inlineCallbacks
    def search(self, pattern):
        I = yield self.getInfo()

        url='%s/getAllPVs?%s'%(I['mgmtURL'],urlencode({'regex':pattern}))

        R = yield self.agent.request('GET', str(url))

        if R.code!=200:
            # spoil cache in case the server changed on us
            self._info = self._info_time = None
            raise RuntimeError('%d: %s'%(R.code, url))

        J = JSONReceiver()
        R.deliverBody(J)
        L = yield J.defer

        defer.returnValue(L)
