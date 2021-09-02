# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

import logging
_log = logging.getLogger(__name__)

import time, re
from fnmatch import filter

from twisted.internet import defer

from ..rpcmunge import NiceProxy as Proxy

class KeyNameMap(object):
    """Hold the pre-configured mapping
    of Client Key name to Server Key patterns
    """
    def __init__(self, D):
        self._orig = D

        self._usr = {} # Client name to Server name patterns
        # Map client names to key numbers
        self._cnames = {} #dict([(n,i) for i,n in enumerate(self._usr)])
        # Map server names to key numbers (unused)
        self._snames = None

        self._namemap = None # Map of client key #s to Server key #s

        for cN, cK, pats in D:
            self._usr[cN] = pats
            self._cnames[cN] = cK

        # Pre-compute reverse name pattern mapping
        R = self._usr_rev = {}
        for cN, _pats in self._usr.items():
            for pat in _pats:
                R[pat] = cN

        assert len(self._usr)>0, "%s"%self._usr
        assert len(self._usr_rev)>0, "%s -> %s"%(self._usr, self._usr_rev)
        assert all(map(len,self._usr_rev.values())), "%s -> %s"%(self._usr, self._usr_rev)

    def dumpClientKeys(self):
        return [{'key':i, 'name':n, 'path':'/dev/random'}
                for n,i,p in self._orig]

    def updateArchives(self, Ks):
        """Provide a new list of server keys
        """
        M = {} # new _namemap

        snames = dict([(K['name'],K['key']) for K in Ks])

        for P, cN in self._usr_rev.items():
            cK = self._cnames[cN]
            cM = M[cK] = []
            for sName in filter(snames, P):
                cM.append(snames[sName])

        _log.info("Recompute Kep map")
        _log.debug("From: %s", snames)
        _log.debug("Gives: %s", M)

        self._snames, self._namemap = snames, M

    def __getitem__(self, k):
        return self._namemap[k]

class InfoCache(object):
    # Max num. of PV for which we hold cached name lookup data
    pvlimit = 500

    # Cache timeout
    timeout = 30

    def __init__(self, url, infomap):
        self.url = url
        P = self.proxy = Proxy(url, limit=10, qlimit=30)
        P.connectTimeout=3.0

        self.flush()

        self._map = infomap
        self.dumpClientKeys = self._map.dumpClientKeys


    def flush(self):
        _log.debug("Key Cache flushed")
        # Results of last archiver.archives call
        self._archives = None
        self._time = 0 # time of last archiver.archives call

        self._pv_cache = {}

    @defer.inlineCallbacks
    def mapKey(self, clientKey):
        """Lookup all server keys from client key
        """
        if time.time()-self._time>=self.timeout:
            _log.debug("Key cache timeout in mapKey: %s", time.time()-self._time)
            R = yield self.proxy.callRemote('archiver.archives')
            self._map.updateArchives(R)
            self._pv_cache = {}
            self._time = time.time()
        else:
            _log.debug("Map cache hit")

        defer.returnValue(self._map[clientKey])

    @defer.inlineCallbacks
    def getKey(self, name, cK):
        """Find the one server key associated with this client key
        where data for the named PV may be found.
        """
        if time.time()-self._time>=self.timeout:
            _log.debug("Key cache timeout in getKey: %s", time.time()-self._time)
            self.flush()

        try:
            CV = self._pv_cache[name][cK]
            _log.debug("Name cache hit: %s %s", name, cK)
            defer.returnValue(CV)
        except KeyError:
            _log.debug("Name cache miss: %s %s", name, cK)
            pass # no info on which server key has our data.

        if len(self._pv_cache)>self.pvlimit:
            #TODO: time based cache
            self._pv_cache = {}

        sKs = yield self.mapKey(cK)

        # devise a regexp to match only this PV
        # TODO: not efficient...
        escname = '^%s$'%re.escape(name)

        cache = {}

        names = yield self.lookup(sKs, escname)
        for sK,R in names.items():
            if len(R)>1:
                _log.warn("name lookup returned several results. %s %s %s", sK, escname, R)

            if len(R):
                cache[cK] = sK
                break

        self._pv_cache[name] = cache
        _log.debug("Update name cache: %s", self._pv_cache)

        defer.returnValue(sK)

    @defer.inlineCallbacks
    def lookup(self, sKs, pat):
        Ds = [self.proxy.callRemote('archiver.names', sK, pat) for sK in sKs]

        Rs = yield defer.DeferredList(Ds, fireOnOneErrback=True,
                                      consumeErrors=True)

        defer.returnValue( dict([(sK,R[1]) for sK,R in zip(sKs,Rs)]) )
