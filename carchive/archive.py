# -*- coding: utf-8 -*-

# supported RPC call version
PVER=0

import re
import logging
_log = logging.getLogger("carchive.archiver")

import time
from xmlrpclib import Fault

from fnmatch import fnmatch
from collections import defaultdict
import numpy as np

from _conf import _conf as defaultConfig
from date import makeTime, timeTuple
from datetime import datetime

from twisted.internet import defer
from twisted.internet.defer import FirstError

# Use EOL hack
from rpcmunge import NiceProxy as Proxy
#from twisted.web.xmlrpc import Proxy

from twisted.internet.error import ConnectionRefusedError

def _optime(R, S):
    E = time.time()
    _log.info("Query complete in %f sec", E-S)
    return R

class HandledError(Exception):
    pass

def _connerror(F):
    if F.check(FirstError):
        F = F.value.subFailure

    if F.check(ConnectionRefusedError):
        _log.fatal("Data server connection refused.  Server not reachable?")
    elif F.check(Fault):
        E = F.value
        if E.faultCode==-600:
            _log.fatal("PV syntax error: %s",E.faultString)
            raise HandledError()
        else:
            _log.fatal("RPC error: %s",E)
    elif F.check(HandledError):
        pass
    else:
        _log.fatal("Remote request failed!  %s",F)
    return F

_dtypes = {
    0: np.dtype('a40'),
    1: np.dtype('a26'),
    2: np.int32,
    3: np.float64
}
dbr_time = [
    ('severity', np.uint32),
    ('status', np.uint16),
    ('sec', np.uint32),
    ('ns', np.uint32)
]

@defer.inlineCallbacks
def getArchive(name=None, conf=defaultConfig):
    """getArchive(conf=...)
    
    Fetch an Archive instance.  If conf is passed
    will be used instead of the default configuration.
    
    Returns a Deferred which will fire with the Archive
    instance.
    """
    if name is None:
        if conf.has_option('DEFAULT', 'defaultarchive'):
	    name=conf.get('DEFAULT', 'defaultarchive')
        else:
            name='DEFAULT'
    url = conf.get(name, 'url')
    maxreq = conf.getint(name, 'maxrequests')
    maxq = conf.getint(name, 'maxquery')
    
    proxy=Proxy(url, limit=maxreq, qlimit=maxq)
    proxy.connectTimeout=3.0

    info = proxy.callRemote('archiver.info').addErrback(_connerror)
    archs= proxy.callRemote('archiver.archives').addErrback(_connerror)
    X = yield defer.DeferredList([info, archs], fireOnOneErrback=True).addErrback(_connerror)
    info, archs = X[0][1], X[1][1]
    
    defer.returnValue(Archive(proxy, conf, info, archs))

class Archive(object):
    """
    """
    def __init__(self, proxy, conf, info, archs):
        self._proxy = proxy
        self.conf = conf
        if PVER < info['ver']:
            _log.warn('Archive server protocol version %d is newer then ours (%d).\n'+
                      'Attempting to proceed.', info['ver'], PVER)

        self.description = info['desc']
        self.statusInfo = dict(enumerate(info['stat']))
        self.severityInfo = {}
        for S in info['sevr']:
            self.severityInfo[int(S['num'])] = S

        self.hows = enumerate(info['how'])
        self.hows = dict([(a,b) for b,a in self.hows])

        # map from name to key
        self.__archs = dict([(x['name'],x['key']) for x in archs])
        # map from key to name
        self.__rarchs = dict([(x['key'],x['name']) for x in archs])

    def severity(self, sevr):
        if sevr==0:
            return ''
        try:
            return self.severityInfo[sevr]['sevr']
        except KeyError:
            return str(sevr)

    def status(self, stat):
        if stat==0:
            return ''
        try:
            return self.statusInfo[stat]
        except IndexError:
            return str(stat)

    def archives(self, pattern):
        return filter(lambda a:fnmatch(a, pattern), self.__archs.iterkeys())

    def lookupArchive(self, arch):
        return self.__rarchs[arch]

    @defer.inlineCallbacks
    def search(self, exact=None, pattern=None,
               archs=None, breakDown=False,
               rawTime=False):
        """Search for PV names matching the given pattern.
        
        If archs is None then all archives are searched.
        Otherwise archs must be a list of strings or integers
        specifing archive names or keys.
        
        Returns a Deferred which fires with a dictionary.
        
        If breakDown is False (the default) then the result is
        {'pvname':(firstTime, lastTime)}
        
        If breakDown is True then the result is
        {'pvname':[(firstTime, lastTime, archKey)]}
        
        In the second form the ranges for each pv will be sorted
        by firstTime.
        
        For either return format, if rawTime is False then a datatime
        is given, otherwise a tuple (sec,nsec).
        """
        if exact is None and pattern is None:
            raise TypeError("Must provide 'exact' or 'pattern'")
        if exact is not None:
            pattern = re.escape(exact)
        else:
            # Test compile to catch basic syntax errors
            re.compile(pattern)

        if archs is None:
            archs = self.__archs.values()
        else:
            for i,a in enumerate(archs):
                try:
                    k = int(a)
                    if k not in self.__archs.itervalues():
                        raise KeyError("Invalid Archive key '%d'"%k)
                    # do nothing
                    continue
                except ValueError:
                    pass
                
                try:
                    k = self.__archs[a]
                    archs[i] = k
                except KeyError:
                    raise KeyError("Invalid Archive key '%s'"%a)

        Ds = [None]*len(archs)
        
        for i,a in enumerate(archs):
            Ds[i] = self._proxy.callRemote('archiver.names', a, pattern).addErrback(_connerror)

        Ds = yield defer.DeferredList(Ds, fireOnOneErrback=True).addErrback(_connerror)

        if breakDown:
            results = defaultdict(list)
            
            for i, (junk, A) in enumerate(Ds):
                for R in A:
                    # Note: Order based on sorting by key name
                    ens, es, ss, sns, pv = R.values()
                    F = (ss, sns)
                    L = (es, ens)
                    if not rawTime:
                        F, L = makeTime(F), makeTime(L)
                    results[pv].append( (F, L, archs[i]) )

            for R in results.itervalues():
                R.sort()

        else:
            results = defaultdict(lambda:[None]*2)
            
            for junk, A in Ds:
                for R in A:
                    # Note: Order based on sorting by key name
                    ens, es, ss, sns, pv = R.values()
                    F = (ss, sns)
                    L = (es, ens)
                    if not rawTime:
                        F, L = makeTime(F), makeTime(L)
                    C = results[pv]
                    if C[0] is None or F < C[0]:
                        C[0] = F
                    if C[1] is None or L > C[1]:
                        C[1] = L

            results = dict([(K,tuple(V)) for K,V in results.iteritems()])

        defer.returnValue(results)

    @defer.inlineCallbacks
    def _fetchdata(self, arch, pv, callback,
                   cbArgs=(), cbKWs={},
                   T0=None, Tend=None,
                   count=None, chunkSize=None,
                   how=0, enumAsInt=False):
        if count is None and chunkSize is None:
            raise TypeError("If count is None then chunkSize must be given")
        if chunkSize is None:
            chunkSize = count
        if T0 is None and Tend is None:
            raise TypeError("Must specify T0 or Tend")
        if T0 is None:
            T0 = datetime.now()
        else:
            T0 = makeTime(T0)
        if Tend is None:
            Tend = datetime.now()
        else:
            Tend = makeTime(Tend)

        if T0 > Tend:
            raise ValueError("T0 must be <= Tend")

        if count is None:
            C = chunkSize
        else:
            C = min(count, chunkSize)

        Tcur = timeTuple(T0)
        Tlast =timeTuple(Tend)
        N = 0
        first = True
        last = False
        while not last and Tcur < Tlast:
            _log.debug('archiver.values(%s,%s,%s,%s,%d,%d)',
                       self.__rarchs[arch],pv,Tcur,Tlast,C,how)
            D = self._proxy.callRemote('archiver.values',
                                       arch, [pv],
                                       Tcur[0], Tcur[1],
                                       Tlast[0], Tlast[1],
                                       C, how).addErrback(_connerror)

            D.addCallback(_optime, time.time())

            try:
                data = yield D
            except:
                _log.fatal('Query fails')
                raise

            assert len(data)==1, "Server returned more than one PVs?"

            assert data[0]['name']==pv, "Server gives us some other PV?"

            vals = data[0]['values']

            maxcount = data[0]['count']

            _log.debug("Query yields %u points"%len(vals))

            N += len(vals)
            last = len(vals)<C
            if count and N>=count:
                last = True

            if data[0]['meta']['type']==0:
                states = data[0]['meta']['states']
            else:
                states = []

            vtype = data[0]['type']
            if vtype==1 and enumAsInt:
                vtype = 2

            try:
                dtype = _dtypes[vtype]
            except KeyError:
                raise ValueError("Server gives unknown value type %d"%vtype)

            XML = data[0]['values']
            
            if len(XML)==0:
                break

            if vtype == 1:
                for V in XML:
                    for i,pnt in enumerate(V['value']):
                        try:
                            V['value'][i] = states[pnt]
                        except IndexError:
                            V['value'][i] = str(pnt)

            maxelem=0
            metadata = np.ndarray(len(XML), dtype=dbr_time)
            for i,E in enumerate(XML):
                maxelem = max(maxelem, len(E['value']))
                metadata[i] = (E['sevr'], E['stat'], E['secs'], E['nano'])

            assert maxcount==maxelem, "Value shape inconsistent. %d %d"%(maxcount,maxelem)

            values = np.ndarray((len(XML), maxelem), dtype=dtype)
            
            for i,E in enumerate(XML):
                V = E['value']
                values[i,:len(V)] = V
                values[i,len(V):] = 0

            del XML
            del data
            
            if first:
                first = False
            else:
                # remove duplicate sample
                values = values[1:]
                metadata = metadata[1:]

            # no non-duplicate samples
            if len(metadata)==0:
                break

            Tcur = (int(metadata[-1]['sec']), int(metadata[-1]['ns']+1))

            callback(values, metadata, *cbArgs, **cbKWs)

        defer.returnValue(N)

    @defer.inlineCallbacks
    def fetchraw(self, pv, callback,
                 cbArgs=(), cbKWs={},
                 T0=None, Tend=None,
                 count=None, chunkSize=None,
                 archs=None, breakDown=None,
                 enumAsInt=False):
        """Fetch raw data for the given PV.

        Results are passed to the given callback as they arrive.
        """
        if breakDown is None:
            breakDown = yield self.search(exact=pv, archs=archs,
                                          breakDown=True, rawTime=True)

        breakDown = breakDown[pv]

        if len(breakDown)==0:
            _log.error("PV not archived")
            defer.returnValue(0)

        Tcur, Tend = timeTuple(T0), timeTuple(Tend)

        _log.debug("Time range: %s -> %s", Tcur, Tend)
        _log.debug("Planning with: %s", map(lambda (a,b,c):(a,b,self.__rarchs[c]), breakDown))

        plan = []
        
        # Plan queries
        # Find a set of non-overlapping regions
        for F, L, K in breakDown:
            if L <= Tcur:
                continue # Too early, keep going
            elif F >= Tend:
                break # No more data in range

            # range to request from this archive
            Rstart = max(Tcur, F)
            Rend   = min(Tend, L)

            plan.append((Rstart, Rend, K))
            
            Tcur =  Rend

        if len(plan)==0 and len(breakDown)>0 and breakDown[-1][1] <= Tcur:
            # requested range is later than last recorded sample,
            # which is all we can return
            F, L, K = breakDown[-1]
            LS, LN = L
            plan.append(((LS+1,0),(LS+2,0),K))
            count=1
            _log.debug("Returning last sample.  No data in or after requested time range.")
        elif len(plan)==0:
            # requested range is earlier than first recorded sample.
            _log.warn("Query plan empty.  No data in or before request time range.")
            defer.returnValue(0)

        _log.debug("Using plan of %d queries %s", len(plan), map(lambda (a,b,c):(a,b,self.__rarchs[c]), plan))

        N = yield self._nextraw(0, pv=pv, plan=plan,
                                Ctot=0, Climit=count,
                                callback=callback, cbArgs=cbArgs,
                                cbKWs=cbKWs, chunkSize=chunkSize,
                                enumAsInt=enumAsInt)

        defer.returnValue(N)

    def _nextraw(self, partcount, pv, plan, Ctot, Climit,
                 callback, cbArgs, cbKWs, chunkSize,
                 enumAsInt):
        sofar = partcount + Ctot
        if len(plan)==0:
            _log.debug("Plan complete: %s", pv)
            return sofar # done
        elif Climit and sofar>=Climit:
            _log.debug("Plan point limit reached: %s", pv)
            return sofar # done

        count = Climit - sofar if Climit else None

        T0, Tend, arch = plan.pop(0)
        _log.debug("Query %d of %s %s -> %s for %s", len(plan), self.__rarchs[arch], T0, Tend, pv)

        D = self._fetchdata(arch, pv, callback,
                            cbArgs=cbArgs, cbKWs=cbKWs,
                            T0=T0, Tend=Tend,
                            count=count,
                            chunkSize=chunkSize,
                            enumAsInt=enumAsInt)

        D.addCallback(self._nextraw, pv, plan, sofar, Climit,
                      callback, cbArgs, cbKWs, chunkSize, enumAsInt)

        return D
