# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger("carchive.appl")

import json, time, calendar, datetime, math, re

from urllib import urlencode

from cStringIO import StringIO

import numpy as np

from twisted.internet import defer, protocol, reactor, threads

from twisted.web.client import Agent, ResponseDone
from twisted.web._newclient import ResponseFailed

from ..date import isoString, makeTime, timeTuple
from ..dtype import dbr_time
from ..status import get_status
from ..util import BufferingLineProtocol
from .EPICSEvent_pb2 import PayloadInfo

from carchive.backend.pbdecode import decoders, unescape, DecodeError, linesplitter

_dtypes = {
    0: np.dtype('a40'),
    1: np.int16,
    2: np.float32,
    3: np.int16, # enum as int
    4: np.int8,
    5: np.int32,
    6: np.float64,
    7: np.dtype('a40'),
    8: np.int16,
    9: np.float32,
    10:np.int16, # enum as int
    11:np.int8,
    12:np.int32,
    13:np.float64,
    14:np.uint8,
}
_dtypes = dict([(k,np.dtype(v)) for k,v in _dtypes.iteritems()])

_is_vect = set([7,8,9,10,11,12,13,14])

class PBReceiver(BufferingLineProtocol):
    """Receive and incrementaionally decode a stream of protobuf.

    nreport is number of samples to accumulate before callback.
    Callback will be invoked when either nreport samples are
    available, or no more samples are availble.

    The PB stream begins with a header line (PayloadInfo) followed by zero
    or more value lines, then possibly a blank line and another header.
    """

    # max number of bytes to accumulate before processing
    #
    # This number must take into account the # of concurrent
    # requests, which by default is 2 per host and we only talk to 1 host.
    # so this number can be set large to better the chance that the entire
    # responce can be processed at once.
    _rx_buf_size = 2**20

    def __init__(self, cb, cbArgs=(), cbKWs={}, nreport=1000,
                 count=None, name=None, cadiscon=0, inthread=False):
        BufferingLineProtocol.__init__(self)
        self._S, self.defer = StringIO(), defer.Deferred()
        self.name, self.nreport, self.cadiscon = name, nreport, cadiscon

        self.header, self._dec, self.name = None, None, name
        self._count_limit, self._count = count, 0
        self._CB, self._CB_args, self._CB_kws = cb, cbArgs, cbKWs
        self.inthread = inthread

    def processLines(self, lines, prev=None):
        _log.debug("Process %d lines for %s", len(lines), self.name)
        if self.inthread:
            return threads.deferToThread(self.process, lines, prev or 0)
        else:
            return self.process(lines, prev or 0)


    def process(self, lines, linesSoFar):
        # group non-empty lines together, empty lines replaced with null
        # eg. 'a\nb\n\nc\nd\n' -> [['a','b'],None,['c','d']]
        parts = linesplitter(lines)

        if len(parts)==0:
            _log.warn("no parts in %d lines?  %s", len(lines), lines[:5])
            return self._count

        for P in parts:
            if P is None:
                # new header will be next
                self.header = None
                continue

            elif len(P)==0:
                continue

            if not self.header:
                # first message in the stream
                self.header = H = PayloadInfo()
                H.ParseFromString(unescape(P[0]))
                try:
                    if H.year<0:
                        H.year = 1 # -1 when no samples available
                    self._year = calendar.timegm(datetime.date(H.year,1,1).timetuple())
                except ValueError:
                    _log.error("Error docoding header: %s %s %s", self.name, H.year, repr(P[0]))
                    raise
                P = P[1:]
            else:
                # use header from previous
                H = self.header

            Nsamp = len(P)
            if not Nsamp:
                continue # header w/o samples...

            elif self._count_limit and self._count+Nsamp>=self._count_limit:
                assert self._count < self._count_limit
                cnt = self._count_limit-self._count
                P = P[:cnt]
                Nsamp = len(P)

            try:
                V, M = decoders[H.type](P, self.cadiscon, self._year)
            except DecodeError as e:
                _log.error("Failed to decode sample %s %s %s", self.name,H.type,repr(e.args[0]))
                raise

            M = np.rec.array(M, dtype=dbr_time)

            M['sec'] += self._year

            #TODO: recheck _count_limit here as len(M)>=Nsamp due to
            #  disconnect events
            self._count += len(M)

            if len(M)==0:
                _log.warn("%s discarding 0 length array %s %s", self.name, V, M)
            else:
                #_log.debug("pushing %s samples: %s", V.shape, self.name)
                if self.inthread:
                    reactor.callFromThread(self._CB, V, M, *self._CB_args, **self._CB_kws)
                else:
                    D = self._CB(V, M, *self._CB_args, **self._CB_kws)
                    assert not isinstance(D, defer.Deferred), "appl does not support callbacks w/ deferred"

            if self._count_limit and self._count>=self._count_limit:
                _log.debug("%s count limit reached", self.name)
                self.transport.stopProducing()
                break

        return self._count

class JSONReceiver(protocol.Protocol):
    """Receive a JSON encoded object

    Decode when entirely received
    """
    def __init__(self):
        self._S, self.defer = StringIO(), defer.Deferred()
    def dataReceived(self, raw):
        self._S.write(raw)
    def connectionLost(self, reason):
        if reason.check(ResponseDone):
            S = self._S.getvalue()
            try:
                J = json.loads(S)
            except ValueError:
                self.defer.errback()
            else:
                self.defer.callback(J)
        else:
            self.defer.errback(reason)

@defer.inlineCallbacks
def fetchJSON(agent, url, code=200):
    R = yield agent.request('GET', str(url))
    if R.code!=code:
        raise RuntimeError("%d: %s"%(R.code,url))

    P = JSONReceiver()
    R.deliverBody(P)
    R = yield P.defer
    defer.returnValue(R)

@defer.inlineCallbacks
def getArchive(conf):
    A = Agent(reactor, connectTimeout=5)

    R = yield A.request('GET', conf['url'])

    if R.code==404:
        raise RuntimeError("Not an Archive Appliance")

    P = JSONReceiver()
    R.deliverBody(P)
    D = yield P.defer

    _log.info("Appliance info: %s", conf['url'])
    for k,v in D.iteritems():
        _log.info(" %s: %s", k,v)

    defer.returnValue(Appliance(A, D, conf))

class Appliance(object):
    def __init__(self, agent, info, conf):
        self._agent, self._info, self._conf = agent, info, conf

    def archives(self, pattern):
        return ['all']

    def lookupArchive(self, arch):
        return 'all'

    
    _severity = {0:'', 1:'MINOR', 2:'Major', 3:'Invalid',
                 3904:'Disconnect', 3872:'Archive_Off', 3848:'Archive_Disable'}

    @classmethod
    def severity(cls, i):
        try:
            return cls._severity[i]
        except KeyError:
            return '<%s>'%i

    @classmethod
    def status(cls, i):
        return get_status(i)

    @defer.inlineCallbacks
    def search(self, exact=None, pattern=None,
               archs=None, breakDown=False,
               rawTime=False):

        assert (exact is None) ^ (pattern is None), 'Only one of exact= or pattern= can be given'
        # ArchiveDataServer looks for partial matches
        # Archive Appliance matches the entire line (implicit ^...$)
        if pattern is None:
            pattern='^%s$'%re.escape(exact)
        elif not pattern:
            pattern = '^.*$'
        else:
            if not pattern.startswith('^') and not pattern.startswith('.*'):
                pattern='.*'+pattern
            if not pattern.endswith('$') and not pattern.endswith('.*'):
                pattern=pattern+'.*'

        url='%s/getAllPVs?%s'%(self._info['mgmtURL'],urlencode({'regex':pattern}))
        _log.debug("Query: %s", url)

        R = yield fetchJSON(self._agent, url)

        if not breakDown:
            meta = makeTime(0), makeTime(time.time())
            R = dict(map(lambda  pv:(pv,meta), R))
        else:
            meta = makeTime(0), makeTime(time.time()), 'all'
            R = dict(map(lambda  pv:(pv,[meta]), R))

        defer.returnValue(R)

    @defer.inlineCallbacks
    def fetchraw(self, pv, callback,
                 cbArgs=(), cbKWs={},
                 T0=None, Tend=None,
                 count=None, chunkSize=None,
                 archs=None, breakDown=None,
                 enumAsInt=False, cadiscon=0):

        Q = {
            'pv':pv,
            'from':isoString(makeTime(T0)),
            'to':isoString(makeTime(Tend)),
        }

        url=str('%s/data/getData.raw?%s'%(self._info['dataRetrievalURL'],urlencode(Q)))
        _log.debug("Query: %s", url)

        R = yield self._agent.request('GET', url)

        if R.code!=200:
            _log.error("%s for %s", R.code, pv)
            defer.returnValue(0)

        P = PBReceiver(callback, cbArgs, cbKWs, name=pv,
                       nreport=chunkSize, count=count, cadiscon=cadiscon)
    
        R.deliverBody(P)
        C = yield P.defer

        defer.returnValue(C)

    def fetchplot(self, pv, callback,
                 cbArgs=(), cbKWs={},
                 T0=None, Tend=None,
                 count=None,
                 **kws):
        # Plot binned queries are rounded to the second and bin size
        # such that the resulting interval is a strict
        # super set of the requested interval

        kws['T0'] = T0
        kws['Tend'] = Tend

        T0, Tend = timeTuple(makeTime(T0))[0], timeTuple(makeTime(Tend))[0]

        if count<=0:
            raise ValueError("invalid sample count (%s <= 0)"%(count,))

        delta = Tend-T0
        N = math.ceil(delta/count) # average sample period

        if N<=1 or delta<=0:
            _log.info("Time range %s too short for plot bin %s, switching to raw", delta, count)
            return self.fetchraw(pv, callback, cbArgs, cbKWs, **kws)

        pv = 'caplotbinning_%d(%s)'%(N,pv)
        return self.fetchraw(pv, callback, cbArgs=cbArgs, cbKWs=cbKWs,
                             **kws)

    def fetchsnap(self, pvs, T=None,
                  archs=None, chunkSize=100,
                  enumAsInt=False):

        raise NotImplementedError("fetchsnap operation not implemented")
