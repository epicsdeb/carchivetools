# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

import re

import numpy

from . import archive, date, _conf, util

__all__ = [
    'arsetdefault',
    'arget',
    'arsearch',
    'EXACT',
    'WILDCARD',
    'REGEXP',
    'RAW',
    'PLOTBIN',
    'SNAPSHOT',
]

# PV name pattern formats
EXACT = 0
WILDCARD = 1
REGEXP = 2

# Data processing formats
RAW = 10
PLOTBIN = 11
SNAPSHOT = 12

_dft_conf = ['DEFAULT']
_reactor = [None]
_servers = {}

def arsetdefault(conf):
    _dft_conf[0] = conf

def getArchive(conf):
    if conf is None:
        conf = _dft_conf[0]
    try:
        return _servers[conf]
    except KeyError:
        pass

    if not _reactor[0]:
        RR = _reactor[0] = archive.ReactorRunner()
        RR.start()

    name = conf
    if not conf or isinstance(conf, str):
        conf = _conf.loadConfig(conf)
    else:
        if 'urltype' not in conf:
            raise ValueError('Invalid configuration')
    S = _reactor[0].call(archive.getArchive, conf)
    _servers[name] = S
    return S

class ResultPV(str):
    """A PV name string augmented with time range information
    """
    start, end = None, None
    breakDown = None

def arsearch(names, match = WILDCARD,
             archs='*', conf=None,
             breakDown=False):
    """Fetch a list of PV names matching the given pattern(s)
    
    Returned PV names are augmented with time range info.
    If breakDown=False (default) then .start and .end
    bound the time range for which data is available.
    If breakDown=True, then .details provides the time ranges
    available in each archive section.
    
    Note that name patterns can be given to arget directly.
    """
    arch = getArchive(conf)
    if isinstance(names, str):
        names = [names]

    if match==EXACT:
        names = ['^'+re.escape(N)+'$' for N in names]
    elif match==WILDCARD:
        names = list(map(util.wild2re, names))
    
    archs = _reactor[0].call(arch.archives, archs)

    res = _reactor[0].callAll([(arch.search, (), {'pattern':N,'archs':archs,'breakDown':breakDown}) for N in names])

    complete = {}
    [complete.update(r) for r in res]

    pvs = set()
    if breakDown:
        for pvname,info in complete.items():
            P = ResultPV(pvname)
            P.breakDown = info
            pvs.add(P)
    else:
        for pvname,info in complete.items():
            P = ResultPV(pvname)
            P.start, P.end = info
            pvs.add(P)

    return pvs

class _Agg(object):
    def __init__(self):
        self.vals, self.metas = [], []
    def __call__(self, data, meta):
        self.vals.append(data)
        self.metas.append(meta)

class _AddPV(object):
    def __init__(self, pv, cb):
        self.pv, self.cb = pv, cb
    def __call__(self, data, meta):
        self.cb(self.pv, data, meta)

def arget(names, match = WILDCARD, mode = RAW,
          start = None, end = None,
          count = None,
          callback = None, chunkSize = 100,
          archs = '*', conf=None,
          enumAsInt=False):
    """Fetch archive data.
    
    The arguments 'names', 'match', and 'archs' have the same meaning as
    with arsearch().
    
    The 'mode' argument selects determines what (if any) post-processing
    is done before data is returned to the caller.
    
    RAW (the default) does no post-processing.  'count' is optional, and
    restricts the maximum number of samples returned.
    
    PLOTBIN applies the Channel Archiver's plot
    binning algorithm to decimate in a way which is pleasing to the eye.
    'count' is mandatory, however, it the number of samples returned may be
    more or less.
    
    The 'start' and 'end' arguments are a pair of times in any of the formats
    accepted by makeTimeInterval().
    
    If enumAsInt=True enumerated values are returned as integers instead
    of strings.
    
    If a 'callback' is provided, then data is passed as callback(values,meta)
    and this function returns None.  Use this for incremental processing
    of large data-sets.
    
    Otherwise:
    
    If only one PV name is given, then a tuple (values, meta) is returned.
    
    If more than on PV name is given, then a dictionary is returned:
        {'pvname':(values, meta)}
        
    In all cases, both values and meta are numpy.ndarray.
    values has the shape [M,N] and metas has [M].  M is the number of time points,
    and N is the maximum number of samples of any time point (N=1 for scalars).
    """
    scalar = False
    if isinstance(names, (str, unicode)):
        scalar, names = True, [names]
    if scalar:
      assert len(names)==1, str(names)

    if mode==PLOTBIN and count is None:
        raise ValueError("PLOTBIN requires sample count")

    start, end = date.makeTimeInterval(start, end)

    arch = getArchive(conf)
    
    archs = _reactor[0].call(arch.archives, archs)

    if mode==RAW:
        def fn(pv, cb):
            return arch.fetchraw(pv, cb, T0=start, Tend=end,
                                 count=count, chunkSize=chunkSize,
                                 enumAsInt=enumAsInt, archs=archs)
    elif mode==PLOTBIN:
        def fn(pv, cb):
            return arch.fetchplot(pv, cb, T0=start, Tend=end,
                                 count=count, chunkSize=chunkSize,
                                 enumAsInt=enumAsInt, archs=archs)
    elif mode!=SNAPSHOT:
        raise ValueError("Unknown plotting mode %d"%mode)

    if match!=EXACT:
        names = arsearch(names, match=match, archs=archs, conf=conf)

    if mode==SNAPSHOT:
        T = date.makeTimeInterval(start, None)[0]
        names = list(map(str, names)) # strip ResultPV
        V, M = _reactor[0].call(arch.fetchsnap, names, T=T,
                                archs=archs, chunkSize=chunkSize)
        return (names, V, M)

    if callback:
        args = [(fn, (str(name), _AddPV(name, callback)), {}) for name in names]
    else:
        args = [(fn, (str(name), _Agg()), {}) for name in names]

    _reactor[0].callAll(args)

    if callback:
        return

    ret = {}
    for _a, (pv, data), _b in args:
        if len(data.vals)==0:
            continue # no data

        vals = numpy.concatenate(data.vals, axis=0)
        meta = numpy.concatenate(data.metas,axis=0)
        ret[pv] = vals, meta

    if scalar and len(names)==1:
        assert len(ret)==1, len(ret)
        return ret[names.pop()]

    return ret
