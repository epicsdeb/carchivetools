# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger("carchive.appl")

import json, time, re, calendar, datetime

from urllib import urlencode

from cStringIO import StringIO

import numpy as np

from twisted.internet import defer, protocol, reactor

from twisted.web.client import Agent, ResponseDone, ResponseFailed

from ..date import isoString
from ..dtype import dbr_time
from . import EPICSEvent_pb2 as pb

# in http replies a new-line is used to seperate samples
# So \n and a few others must be escaped to prevent confusion.
# Escapes are a two charactor sequence with \x1b (ascii ESC)
# followed by an index.  Currently 1 (ESC), 2 (NL), and 3 (CR)
# are defined.  Others are invalid.
_esc = re.compile('\x1b(.)', re.DOTALL)
_esc_chr = [None, '\x1b', '\n', '\r']

def _esc_fn(M):
    return _esc_chr[ord(M.group(1))]

def unescape(S):
    return _esc.sub(_esc_fn, S)

# Proto buffer instances for decoding individual samples
_fields = {
    0:pb.ScalarString,
    1:pb.ScalarShort,
    2:pb.ScalarFloat,
    3:pb.ScalarEnum,
    4:pb.ScalarByte,
    5:pb.ScalarInt,
    6:pb.ScalarDouble,
    7:pb.VectorString,
    8:pb.VectorShort,
    9:pb.VectorFloat,
    10:pb.VectorEnum,
    #11:pb.VectorByte, # missing?
    12:pb.VectorInt,
    13:pb.VectorDouble,
    14:pb.V4GenericBytes,
}


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

_is_vect = set([7,8,9,10,11,12,13,14])

class PBReceiver(protocol.Protocol):
    """Receive an incrementaionally decode a stream of protobuf.

    nreport is number of samples to accumulate before callback.
    Callback will be invoked when either nreport samples are
    available, or no more samples are availble.
    """

    # max number of bytes to accumulate before processing
    #
    # This number must take into account the # of concurrent
    # requests, which by default is 2 per host and we only talk to 1 host.
    # so this number can be set large to better the chance that the entire
    # responce can be processed at once.
    _rx_buf_size = 10*2**20

    def __init__(self, cb, cbArgs=(), cbKWs={}, nreport=1000, count=None, name=None):
        self._S, self.defer = StringIO(), defer.Deferred()
        self.name, self.nreport = name, nreport

        self._B = StringIO() # partial line buffer

        # trick StringIO to allocate the full buffer size
        # to allow append w/o re-alloc
        self._B.seek(self._rx_buf_size+1024)
        self._B.write('x')
        self._B.truncate(0)

        self.header, self._dec, self.name = None, None, name
        self._count_limit, self._count = count, 0
        self._CB, self._CB_args, self._CB_kws = cb, cbArgs, cbKWs

    def dataReceived(self, raw, flush=False):
        try:
            self._B.write(raw)
            if self._B.tell() < self._rx_buf_size and not flush:
                return

            L = self._B.getvalue().split('\n')
            self._B.truncate(0)
            self._B.write(L[-1]) # any bytes after the last newline (partial message)

            self.process(L[:-1])
        except:
            _log.exception("dataReceived")
            raise

    def connectionLost(self, reason):
        if self._count_limit and self._count>=self._count_limit and reason.check(ResponseFailed):
            _log.debug("Lost connection after data count reached")
            self.defer.callback(self._count)

        elif reason.check(ResponseDone):
            _log.debug("All available samples received for %s", self.name)
            if self._B.tell()>0:
                self.dataReceived('', flush=True)
            self.defer.callback(self._count)

        else:
            _log.error("Connection lost while reading %s (%s)", self.name, reason)
            self.defer.errback(reason)

    # Internal methods

    def allocArrays(self, L):
        H = self.header
        dtype = _dtypes[H.type]
        
        # TODO: not optimal for arrays as the 2nd dim will certainly
        # be too small and we will re-allocate when decoding the first
        # actual sample
        V = np.ndarray((L,1), dtype)

        M = np.ndarray(L, dtype=dbr_time)
        return V, M

    def process(self, lines):
        total = len(lines)
        V = M = None
        if self.header is not None:
            # optimistically assume that all lines are data lines
            V, M = self.allocArrays(total)
            N = 0

        for i,L in enumerate(map(unescape, lines)):
            if not L:
                self.header, self._dec = None, None
                continue

            # select which protobuf decoder to use
            if self.header:
                D = self._dec()
            else:
                OH = self.header # save old header
                D = self.header = pb.PayloadInfo()

            try:
                D.ParseFromString(L)
            except:
                _log.fatal("Error Decoding %s", repr(L))
                raise

            if D is self.header:
                # New header received
                self._year = calendar.timegm(datetime.date(D.year,1,1).timetuple())
                self._dec = _fields[D.type] # lookup protobuf decoder

                if OH is not None and OH.type != D.type:
                    _log.warn("PV %s change type from %d to %d",
                              self.name, OH.type, D.type)
                    self.pushCB(V[:N], M[:N])
                    V, M = self.allocArrays(total-i)
                    N = 0

                elif V is None:
                    V, M = self.allocArrays(total-i)
                    N = 0

            else:
                # New sample

                if self.header.type not in _is_vect:
                    V[N] = D.val
                else:
                    if len(D.val)>V.shape[1]:
                        V = self._buf_val = np.resize((V.shape[0], len(D.val)))
                    V[N,:len(D.val)] = D
                    V[N,:len(D.val)] = 0

                M[N]['sec'] = self._year + D.secondsintoyear
                M[N]['ns'] = D.nano
                M[N]['severity'] = D.severity
                M[N]['status'] = D.status
                N += 1
                self._count += 1
                if self._count_limit and self._count >= self._count_limit:
                    _log.info("Count limit met for %s (%d)", self.name, self._count_limit)
                    self.pushCB(V[:N], M[:N])
                    self.transport.stopProducing()
                    return

        self.pushCB(V[:N], M[:N])

    def pushCB(self, V, M):
        if len(M)==0:
            _log.warn("%s discarding 0 length array %s %s", V, M)
            return
        _log.debug("pushing %s samples: %s", V.shape, self.name)
        self._CB(V, M, *self._CB_args, **self._CB_kws)

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
            J = json.loads(S)
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
    A = Agent(reactor)

    R = yield A.request('GET', conf['url'])

    if R.code==404:
        raise RuntimeError("Not an Archive Appliance")

    P = JSONReceiver()
    R.deliverBody(P)
    D = yield P.defer

    _log.info("Appliance info")
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

    
    _severity = {0:'', 1:'MINOR', 2:'Major', 3:'Invalid'}

    @classmethod
    def severity(cls, i):
        return cls._severity.get(i, '<unknown>')
    @classmethod
    def status(cls, i):
        if i==0:
            return ''
        return str(i) #TODO: real status names

    @defer.inlineCallbacks
    def search(self, exact=None, pattern=None,
               archs=None, breakDown=False,
               rawTime=False):

        url='%s/getAllPVs?%s'%(self._info['mgmtURL'],urlencode({'pv':pattern}))
        _log.debug("Fetch: %s", url)

        R = yield fetchJSON(self._agent, url)

        if not breakDown:
            meta = 0, time.time()
            R = dict(map(lambda  pv:(pv,meta), R))
        else:
            meta = 0, time.time(), 'all'
            R = dict(map(lambda  pv:(pv,[meta]), R))

        defer.returnValue(R)

    @defer.inlineCallbacks
    def fetchraw(self, pv, callback,
                 cbArgs=(), cbKWs={},
                 T0=None, Tend=None,
                 count=None, chunkSize=None,
                 archs=None, breakDown=None,
                 enumAsInt=False):

        Q = {
            'pv':pv,
            'from':isoString(T0),
            'to':isoString(Tend),
            'donotchunk':'true',
        }
        _log.debug("Query %s", Q)

        url=str('%s/data/getData.raw?%s'%(self._info['dataRetrievalURL'],urlencode(Q)))

        R = yield self._agent.request('GET', url)

        if R.code==404:
            raise RuntimeError("%d: %s"%(R.code,url))

        P = PBReceiver(callback, cbArgs, cbKWs, name=pv,
                       nreport=chunkSize, count=count)
    
        R.deliverBody(P)
        C = yield P.defer

        defer.returnValue(C)
