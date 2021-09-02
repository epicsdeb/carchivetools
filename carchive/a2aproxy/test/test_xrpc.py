# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""



import sys

from twisted.trial import unittest

if sys.version_info<(2,7):
    raise unittest.SkipTest('Not supported for python 2.6')

try:
    from io import StringIO
    from xmlrpc.client import loads, dumps, Fault
except ImportError:
    from cStringIO import StringIO
    from xmlrpclib import loads, dumps, Fault


from twisted.web.xmlrpc import Proxy

from twisted.internet import defer, reactor
from twisted.web.server import Site

from .. import resource, xrpcrequest

class TestRequest(object):
    code = 200
    def __init__(self, content=None):
        self.content = StringIO(content)
        self.Ds, self.Hs = [], {}
        self.data = StringIO()
        self.write = self.data.write
    def setResponseCode(self, code):
        self.code = code
    def setHeader(self, K, V):
        self.Hs[K] = [V]
    def notifyFinish(self):
        self.Ds.append(defer.Deferred())
        return self.Ds[-1]
    def finish(self):
        Ds, self.Ds = self.Ds, None
        for D in Ds:
            D.callback(None)
    def connectionLost(self, reason):
        Ds, self.Ds = self.Ds, None
        if not Ds:
            return
        for D in Ds:
            D.errback(reason)

class TestReq(unittest.TestCase):
    def setUp(self):
        self.S = resource.DataServer()

    def test_info(self):
        R = TestRequest(content=dumps((), 'archiver.info'))
        X = self.S.render_POST(R)
        self.assertEqual(X, resource._info_rep)
        self.assertEqual(R.code, 200)

    def test_archives(self):
        R = TestRequest(content=dumps((), 'archiver.archives'))
        X = self.S.render_POST(R)
        self.assertEqual(X, resource._archives_rep)
        self.assertEqual(R.code, 200)

class TestServer(unittest.TestCase):
    timeout = 1.0
    def setUp(self):
        self.root = resource.buildResource('127.0.0.1:99999')
        self.site = Site(self.root) # TODO: what is timeout?
        self.serv = reactor.listenTCP(0, self.site, interface='127.0.0.1')
        P = self.serv.getHost().port
        url = 'http://127.0.0.1:%d/cgi-bin/ArchiveDataServer.cgi'%P
        self.client = Proxy(url)

    def tearDown(self):
        self.serv.stopListening()

    @defer.inlineCallbacks
    def test_info(self):
        R = yield self.client.callRemote('archiver.info')
        self.assertEqual(R ,resource._info)

    @defer.inlineCallbacks
    def test_archives(self):
        R = yield self.client.callRemote('archiver.archives')
        self.assertEqual(R ,resource._archives)

class TestValuesEncoder(unittest.TestCase):
    _data = [
        (0, 'test'),
        (2, 42),
        (3, 4.5),
    ]

    def test_no_pvs(self):
        A = xrpcrequest._values_start + xrpcrequest._values_end
        X = loads(A)[0][0]
        self.assertEqual(X,[])

    def test_no_samples(self):
        A = [xrpcrequest._values_start,
             xrpcrequest._values_head%{'name':'pvx','type':42,'count':43},
             xrpcrequest._values_foot,
             xrpcrequest._values_end,
            ]
        X = loads(''.join(A))[0][0]
        self.assertEqual(X,[{'count': 43,
                          'meta': {'alarm_high': 0.0,
                                   'alarm_low': 0.0,
                                   'disp_high': 0.0,
                                   'disp_low': 0.0,
                                   'prec': 0,
                                   'type': 1,
                                   'units': '',
                                   'warn_high': 0.0,
                                   'warn_low': 0.0},
                          'name': 'pvx',
                          'type': 42,
                          'values': []}])

    def test_encode(self):
        for type, val in self._data:
            try:
                v = xrpcrequest._encoder[type](val)
                A = [xrpcrequest._values_start,
                     xrpcrequest._values_head%{'name':'pvx','type':42,'count':43},
                     xrpcrequest._sample_head%{'stat':1, 'sevr':2, 'secs':3, 'nano':4},
                     xrpcrequest._sample_start,
                     v,
                     xrpcrequest._sample_foot,
                     xrpcrequest._values_foot,
                     xrpcrequest._values_end,
                    ]

                A = ''.join(A)
                X = loads(A)[0][0]

                self.assertEqual(X, [{'count': 43,
                                      'meta': {'alarm_high': 0.0,
                                               'alarm_low': 0.0,
                                               'disp_high': 0.0,
                                               'disp_low': 0.0,
                                               'prec': 0,
                                               'type': 1,
                                               'units': '',
                                               'warn_high': 0.0,
                                               'warn_low': 0.0},
                                      'name': 'pvx',
                                      'type': 42,
                                      'values': [{'nano': 4, 'secs': 3, 'sevr': 2, 'stat': 1, 'value': [val]}],
                                     }])

            except:
                print('Error in',type,val)
                raise
