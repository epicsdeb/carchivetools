# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger("carchive.appl")

import json, time, calendar, datetime

from urllib import urlencode

from cStringIO import StringIO

import numpy as np

from twisted.internet import defer, protocol, reactor

from twisted.web.client import Agent, ResponseDone
from twisted.web._newclient import ResponseFailed

from ..date import isoString
from ..dtype import dbr_time
from .EPICSEvent_pb2 import PayloadInfo

from carchive.backend.pbdecode import decoders, unescape, DecodeError

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
    _rx_buf_size = 2**20

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
            self.transport.stopProducing()
            _log.exception("dataReceived")
            return

    def connectionLost(self, reason):
        if self._count_limit and self._count>=self._count_limit and reason.check(ResponseFailed):
            _log.debug("Lost connection after data count reached")
            self.defer.callback(self._count)

        elif reason.check(ResponseDone):
            _log.debug("%s samples received for %s", self._count, self.name)
            try:
                if self._B.tell()>0:
                    self.dataReceived('', flush=True)
            except:
                self.defer.errback()
            else:
                self.defer.callback(self._count)

        else:
            _log.error("Connection lost while reading %s (%s)", self.name, reason)
            self.defer.errback(reason)

    # Internal methods

    def process(self, lines):
        lines = map(unescape, lines)
        # find the index of blank lines which preceed new headers
        # These are assumed to be relatively rare (so 'splits' is short)
        #
        # 'splits' will be a list of indicies of blank lines
        splits = map(lambda (a,b):a, filter(lambda (i,x):len(x)==0, enumerate(lines)))
        # break up the single list of lines into a list of lists
        # where eash sub-list where the first element is a header (except for the first)
        # and the remaining lines are all of the same type
        parts = map(lambda (a,b):lines[a+1:b], zip([-1] + splits, splits + [None]))
        
        dparts = map(lambda (a,b):(a+1,b), zip([-1] + splits, splits + [None]))
        _log.debug("Parts: %s", dparts)

        if len(parts)==0:
            _log.warn("no parts in %d lines?  %s", len(lines), lines[:5])
            return

        for P,dP in zip(parts,dparts):
            if len(P)==0:
                _log.warn("Part with no lines? %s", P)
                continue

            if not self.header:
                # first message in the stream
                H = PayloadInfo()
                H.ParseFromString(P[0])
                try:
                    if H.year<0:
                        H.year = 1 # -1 when no samples available
                    self._year = calendar.timegm(datetime.date(H.year,1,1).timetuple())
                except ValueError:
                    _log.error("Error docoding: %s %s", H.year, repr(P[0]))
                    print H
                    raise
                P = P[1:]
            else:
                # reuse header (interrupted stream)
                H, self.header = self.header, None

            Nsamp = len(P)
            if not Nsamp:
                continue # header w/o samples...

            elif self._count_limit and self._count+Nsamp>=self._count_limit:
                assert self._count < self._count_limit
                cnt = self._count_limit-self._count
                P = P[:cnt]
                Nsamp = len(P)

            decode = decoders[H.type]
            V = np.ndarray((Nsamp,1), dtype=_dtypes[H.type])
            M = np.ndarray((Nsamp,), dtype=dbr_time)

            I = 0
            while I<Nsamp:
                try:
                    Ix, L = decode(P[I:], V[I:], M[I:])
                except DecodeError as e:
                    raise DecodeError("Failed to decode %s of %s: %s"%(e.args,dP,repr(P[e.args[0]])))
                assert Ix>0 or I==0
                I += Ix
                assert L is None or I<len(M)
                if L is not None:
                    # Must extend 2nd dim
                    # V.resize pads with zeros
                    V.resize((V.shape[0], L))

            M['sec'] += self._year

            self._count += Nsamp

            if len(M)==0:
                _log.warn("%s discarding 0 length array %s %s", self.name, V, M)
            else:
                #_log.debug("pushing %s samples: %s", V.shape, self.name)
                self._CB(V, M, *self._CB_args, **self._CB_kws)

            if self._count_limit and self._count>=self._count_limit:
                _log.info("%s count limit reached", self.name)
                self.transport.stopProducing()
                break
        self.header = H

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

        # ArchiveDataServer looks for partial matches
        # Archive Appliance matches the entire line (implicit ^...$)
        if not pattern:
            pattern='.*'
        else:
            if not pattern.startswith('^'):
                pattern='.*'+pattern
            if not pattern.endswith('$'):
                pattern=pattern+'.*'

        url='%s/getAllPVs?%s'%(self._info['mgmtURL'],urlencode({'regex':pattern}))
        _log.debug("Query: %s", url)

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
        }

        url=str('%s/data/getData.raw?%s'%(self._info['dataRetrievalURL'],urlencode(Q)))
        _log.debug("Query: %s", url)

        R = yield self._agent.request('GET', url)

        if R.code==404:
            raise RuntimeError("%d: %s"%(R.code,url))

        P = PBReceiver(callback, cbArgs, cbKWs, name=pv,
                       nreport=chunkSize, count=count)
    
        R.deliverBody(P)
        C = yield P.defer

        defer.returnValue(C)
