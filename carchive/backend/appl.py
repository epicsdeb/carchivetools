# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger("carchive.appl")

import json, time, calendar, datetime, math, re

from urllib import urlencode

from cStringIO import StringIO

import numpy as np

from twisted.internet import defer, protocol, reactor

from twisted.web.client import Agent, ResponseDone
from twisted.web._newclient import ResponseFailed

from ..date import isoString, makeTime
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
    """Receive and incrementaionally decode a stream of protobuf.

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

    def __init__(self, cb, cbArgs=(), cbKWs={}, nreport=1000,
                 count=None, name=None, cadiscon=0):
        self._S, self.defer = StringIO(), defer.Deferred()
        self.name, self.nreport, self.cadiscon = name, nreport, cadiscon

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
            try:
                if self._B.tell()>0:
                    self.dataReceived('', flush=True)
            except:
                self.defer.errback()
            else:
                _log.debug("%s samples received for %s", self._count, self.name)
                self.defer.callback(self._count)

        else:
            _log.error("Connection lost while reading %s (%s)", self.name, reason)
            self.defer.errback(reason)

    # Internal methods

    def process(self, lines):
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
                H.ParseFromString(unescape(P[0]))
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

            try:
                V, M = decoders[H.type](P, self.cadiscon)
            except DecodeError as e:
                raise ValueError("Failed to decode: "+repr(e.args[0]))
            M = np.rec.array(M, dtype=dbr_time)

            M['sec'] += self._year

            self._count += Nsamp

            if len(M)==0:
                _log.warn("%s discarding 0 length array %s %s", self.name, V, M)
            else:
                #_log.debug("pushing %s samples: %s", V.shape, self.name)
                D = self._CB(V, M, *self._CB_args, **self._CB_kws)
                assert not isinstance(D, defer.Deferred), "appl does not support callbacks w/ deferred"

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
            pattern=re.escape(exact)
        else:
            if not pattern.startswith('^'):
                pattern='.*'+pattern
            if not pattern.endswith('$'):
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
            'from':isoString(T0),
            'to':isoString(Tend),
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

    _binops = [
        ('firstSample_%d(%s)', 0),
        ('minSample_%d(%s)', 1),
        ('maxSample_%d(%s)', 2),
        ('lastSample_%d(%s)', 3),
    ]

    @defer.inlineCallbacks
    def fetchplot(self, pv, callback,
                 cbArgs=(), cbKWs={},
                 T0=None, Tend=None,
                 count=None,
                 **kws):
        kws['T0'] = T0
        kws['Tend'] = Tend

        delta = (Tend-T0).total_seconds()
        if delta<=0.0 or count<=0:
            raise ValueError("invalid time range or sample count (%s <= 0 or %s <= 0"%(delta,count))

        N = math.ceil(delta/count) # average sample period

        if N<1:
            _log.info("Time range too short for plot bin, switching to raw")
            D = self.fetchraw(pv, callback, cbArgs, cbKWs, **kws)
            defer.returnValue(D)

        pieces = [None]*len(self._binops)
        def storeN(values, metas, i):
            if values.shape[1]!=1:
                raise ValueError("fetchplot not defined for waveforms")
            values = values[:,0]
            if pieces[i]:
                values = np.concatenate((pieces[i][0], values), axis=0)
                metas  = np.concatenate((pieces[i][1], metas), axis=0)
            pieces[i] = values, metas

        Ds = [None]*len(pieces)
        for pat, i in self._binops:
            Ds[i] = self.fetchraw(pat%(N,pv), storeN, cbArgs=(i,), cbKWs={}, **kws)
        
        yield defer.gatherResults(Ds, consumeErrors=True)

        if any(map(lambda x:x is None, pieces)):
            defer.returnValue(0) # no data

        # First, mInimum, mAximum, Last
        # each is a pair of (values, metas)
        F, I, A, L = pieces

        if len(F[1])==len(I[1])+1:
            F = F[0][1:], F[1][1:]
            L = L[0][1:], L[1][1:]

#        print 'LLL',[len(P[1]) for P in pieces]
#        for xx in pieces:
#            print 'V',xx[0]
#            print 'M',xx[1]

        assert F[1].shape==I[1].shape
        assert F[1].shape==A[1].shape
        assert F[1].shape==L[1].shape

        # find bins with one or two samples
        fsa = F[1]==I[1] # first sample is max
        fsi = F[1]==A[1] # first sample is min

        one = fsa&fsi # first sample is max and min (and last)
        two = fsa^fsi # first sample is max or min, but not both
        many= ~(one|two)

        # the number of output samples for each bin
        mapping = np.ndarray((len(one),1), dtype=np.int8)
        mapping[one] = 1
        mapping[two] = 2
        mapping[many]= 4
        
        # index of the first output sample of each bin
        idx = np.ndarray((len(one),), dtype=np.int32)
        idx[0]=0
        idx[1:] = mapping[:-1].cumsum()

        nsamp = mapping.sum() # total num. of output samples
        
        assert idx[-1]+mapping[-1]==nsamp

        values = np.ndarray((nsamp,1), dtype=F[0].dtype)
        metas  = np.ndarray((nsamp,), dtype=F[1].dtype)
        
        # bins with one sample simply pass through that sample
        if np.any(one):
            values[idx[one],0] = F[0][one]
            metas[idx[one]]    = F[1][one]

        # bins w/ two samples are just as easy
        if np.any(two):
            values[idx[two],0]  = I[0][two]
            metas[idx[two]]     = I[1][two]
            values[idx[two]+1,0]= A[0][two]
            metas[idx[two]+1]   = A[1][two]

        # bins with more than two samples are more complex

        # Start by copying through
        if np.any(many):
            print 'Q',values[idx[many]]
            values[idx[many],0]  = F[0][many]
            metas[idx[many]]     = F[1][many]
            values[idx[many]+1,0]= I[0][many]
            metas[idx[many]+1]   = I[1][many]
            values[idx[many]+3,0]= L[0][many]
            metas[idx[many]+3]   = L[1][many]
            values[idx[many]+2,0]= A[0][many]
            metas[idx[many]+2]   = A[1][many]
            
            # place min/max samples at times 1/3 and 2/3 between first and last
    
            T0 = F[1]['sec'][many]+1e-9*F[1]['ns'][many]
            T1 = L[1]['sec'][many]+1e-9*L[1]['ns'][many]
            dT = (T1-T0)/3.0
    
            TI = T0 + dT
            TA = TI + dT
    
            metas['sec'][idx[many]+1] = np.asarray(TI, dtype=np.uint32)
            metas['sec'][idx[many]+2] = np.asarray(TA, dtype=np.uint32)
    
            metas['ns'][idx[many]+1] = np.asarray((TI*1e9)%1e9, dtype=np.uint32)
            metas['ns'][idx[many]+2] = np.asarray((TA*1e9)%1e9, dtype=np.uint32)

        callback(values, metas, *cbArgs, **cbKWs)

        defer.returnValue(nsamp)
