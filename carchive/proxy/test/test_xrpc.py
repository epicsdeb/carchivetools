# -*- coding: utf-8 -*-

from cStringIO import StringIO
from xmlrpclib import dumps, loads, Fault

from twisted.web.xmlrpc import Proxy

from twisted.internet import defer, reactor
from twisted.web.server import Site
from twisted.trial import unittest

from .. import resource, xrpcrequest

class TestNamesRequest(xrpcrequest.XMLRPCRequest):
    # key, pattern
    argumentTypes = (int, str)
    def __init__(self, httprequest, args, applinfo=None):
        super(TestNamesRequest, self).__init__(httprequest, args)
        R = dumps([
            {'name':'testpv',
             'start_sec':42, 'start_nano':1234,
             'end_sec':45, 'end_nano':5678},
        ])
        self.request.write(dumps(R, methodresponse=True))
        self.request.finish()

class TestValuesRequest(xrpcrequest.XMLRPCRequest):
    # key, names, start_sec, start_nano, end_sec, end_nano, count, how
    argumentTypes = (int, list, int, int, int, int, int, int)
    def __init__(self, httprequest, args, applinfo=None):
        super(TestValuesRequest, self).__init__(httprequest, args)
        R = []
        for name in self.args[1]:
            R.append(
                {'name':name, 'type':2,
                 'meta':{'type':1,
                         'disp_high':0, 'disp_low':0,
                         'alarm_high':0, 'alarm_low':0,
                         'warn_high':0, 'warn_low':0,
                         'prec':0, 'units':'',
                         },
                 'count':2,
                 'values':[
                     {'stat':0, 'sevr':0, 'secs':self.args[2],
                      'nano':self.args[3], 'values':[1]},
                     {'stat':0, 'sevr':0, 'secs':self.args[4],
                      'nano':self.args[5], 'values':[2]},
                 ],
                }
            )
        self.request.write(dumps(R, methodresponse=True))
        self.request.finish()


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
        self.S.NamesRequest = TestNamesRequest
        self.S.ValuesRequest = TestValuesRequest

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
        self.root = resource.buildResource()
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
