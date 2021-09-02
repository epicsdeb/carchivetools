# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

import logging
_log = logging.getLogger(__name__)

import weakref, time

from zope.interface import implementer

try:
    from xmlrpc.client import loads, dumps, Fault
except ImportError:
    from xmlrpclib import loads, dumps, Fault

from twisted.internet import defer, protocol
from twisted.web.iweb import IBodyProducer
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web.http_headers import Headers

from ..util import LimitedAgent

@implementer(IBodyProducer)
class StringProducer(object):
    def __init__(self, body):
        self.body = body
        self.length = len(body)
    def startProducing(self, consumer):
        consumer.write(self.body)
        return defer.succeed(None)
    def pauseProducing(self):
        pass
    def stopProducing(self):
        pass

class ReverseProxyProducer(protocol.Protocol):
    """Proxy between an HTTP Client (acting as IProtcol)
    and an HTTP Server (acting as IPushProducer)
    """

    def __init__(self, req):
        self._req, self._done, self._buf = req, False, ''
        self._paused = False
        self.defer = defer.Deferred()
        req.registerProducer(self, True)

    # IPushProducer methods
    # Called by server Request
    def pauseProducing(self):
        self._paused = True
        self.transport.pauseProducing()

    def resumeProducing(self):
        assert not self._done
        self._paused = False
        if self._buf:
            self._req.write(self._buf)
            self._req.write(self._buf)
            self._buf = ''
        if not self._paused:
            self.transport.resumeProducing()

    def stopProducing(self):
        if self._done:
            return
        self._done, self._paused = True, True
        self.transport.stopProducing()
        self._req.unregisterProducer()
        self._req.finish()
        self.defer.callback(None)

    # IProtocol methods
    # Called by client
    def dataReceived(self, raw):
        if self._paused:
            self._buf += raw
        else:
            if self._buf:
                self._req.write(self._buf)
                self._buf = ''
            self._req.write(raw)

    def connectionLost(self, reason):
        if not self._done:
            self._req.unregisterProducer()
            self._req.finish()
            self.transport.stopProducing()
            self.defer.callback(None)
        self._done, self._paused = True, True

def cleanupRequest(R, req):
    if not req.startedWriting:
        req.setResponseCode(500)
        req.write("")
    if not req.finished:
        req.finish()
    return R

_msg = """<html><body><h1>Archive Data Server middleware</h1>
<pre>
%d requests in progress.
%d/%d PVs in cache.
Cache age: %s sec.
Cache expires after: %s sec.
</pre>
</html></body>
"""

class XMLRPCProxy(Resource):
    isLeaf=True

    def render_GET(self, req):
        return _msg%(len(self.requests),
                     len(self.info._pv_cache), self.info.pvlimit,
                     time.time()-self.info._time, self.info.timeout
                     )

    def render_POST(self, req):
        self.requests[req] = None # store weakref to track active requests

        if req.content is None:
            req.setResponseCode(405)
            return 'Missing request body'

        try:
            rawreq = req.content.read()
            args, meth = loads(rawreq)
            _log.info("Request: %s%s", meth,args)

            req.setHeader('Content-Type', 'text/xml')

            if meth == 'archiver.archives':
                return dumps((self.info.dumpClientKeys(),), methodresponse=True)
            elif meth == 'archiver.names':
                D =self._names(req, args)
            elif meth == 'archiver.values':
                D =self._values(req, args)
            else:
                D =self._proxy(req, rawreq)

        except Exception as e:
            import traceback
            traceback.print_exc()
            req.setResponseCode(400)
            req.write("")
            return e.message

        D.addBoth(cleanupRequest, req)

        return NOT_DONE_YET

    @defer.inlineCallbacks
    def _names(self, req, args):
        """Search on all server keys associated with the requested client key
        Merge the results while avoiding duplicates.
        """

        cK, pat = args
        sKs = yield self.info.mapKey(cK)

        results = {}
        names = yield self.info.lookup(sKs, pat)

        for sK, PVs in names.items():
            for pv in PVs:
                results[pv['name']] = pv

        R = dumps((list(results.values()),), methodresponse=True)
        req.write(R)
        req.finish()

    @defer.inlineCallbacks
    def _values(self, req, args):
        """Find the one server key holding data for each requested PV
        """
        cK, names = args[:2]
        sKs = {}
        for pv in names:
            sK = yield self.info.getKey(pv, cK)
            try:
                sKs[sK].append(pv)
            except KeyError:
                sKs[sK] = [pv]

        if len(sKs)!=1:
            R = dumps(Fault(308, "archiver.values w/ PVs in several sections not supported"),
                      methodresponse=True)
            req.write(R)
            req.finish()

        else:
            args = (sK,) + args[1:]

            rawreq = dumps(args, methodname='archiver.values')
            yield self._proxy(req, rawreq)

    @defer.inlineCallbacks
    def _proxy(self, req, rawreq):
        post = StringProducer(rawreq)

        D = yield self.agent.request('POST', self.info.url,
                                     Headers({'Content-Type':['text/xml']}),
                                     bodyProducer=post)
        if D.code!=200:
            raise RuntimeError("Request fails %d: %s -> %s"%(D.code, rawreq, self.info.url))

        _log.debug("%d: %s", D.code, loads(rawreq))

        P = ReverseProxyProducer(req)
        D.deliverBody(P)
        yield P.defer

        defer.returnValue(None)

def buildResource(info=None, reactor=None):
#    I = InfoCache(rpcurl, mapconf)
    root= Resource()
    cgibin = Resource()
    root.putChild('cgi-bin', cgibin)
    C = XMLRPCProxy()
    cgibin.putChild('ArchiveDataServer.cgi', C)

    C.info = info
    C.agent = LimitedAgent(reactor)
    C.requests = weakref.WeakKeyDictionary()

    return root, C
