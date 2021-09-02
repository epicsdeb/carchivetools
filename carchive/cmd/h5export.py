# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

import logging
_log = logging.getLogger("carchive.h5export")

import h5py

import numpy as np

from twisted.internet import defer

from carchive.date import makeTimeInterval, timeTuple
from carchive.dtype import dbr_time

def printData(data, meta, archive, info):
    metaset = info.metaset
    assert len(meta)>0, 'Empty dataset'
    
    if not info.valset: # first data
        pvstore = info.pvstore
        valset = pvstore.get('value')
        if valset is None:
            valset = pvstore.create_dataset('value',
                                            shape = (0,0),
                                            dtype=data.dtype,
                                            maxshape=(None,None),
                                            chunks=data.shape,
                                            shuffle=True,
                                            compression='gzip')
        info.valset = valset
    else: # additional data
        valset = info.valset

    if metaset.shape[0]:
        lastsamp = (metaset['sec'][-1], metaset['ns'][-1])
        newstart = (meta['sec'][0], meta['ns'][0])  
        
        if(lastsamp >= newstart):
            _log.info('Ignoring overlapping data. %s >= %s', lastsamp, newstart)
            return  

    mstart = info.metaset.shape[0]
    info.metaset.resize((mstart+meta.shape[0],))
    
    info.metaset[mstart:] = meta
    
    start = valset.shape[0]
    
    shape = (valset.shape[0] + data.shape[0],
             max(valset.shape[1], data.shape[1]))

    valset.resize(shape)

    if valset.dtype!=data.dtype:
        if valset.dtype.kind in ['i','f'] and data.dtype.kind in ['i','f']:
            pass # silently cast between float and int
        else:
            _log.warning("Can't cast from %s to %s.  Ignoring samples.",
                         data.dtype, valset.dtype)
            return

    # (serialized) callbacks on multiple threads triggers "warnings"
    # fixed in h5py >= 2.1.1
    # http://groups.google.com/forum/?fromgroups=#!topic/h5py/fS5398h3cQI
    valset[start:,:data.shape[1]] = data

    _log.debug("%s total samples for %s", valset.shape, info.pv)

class printInfo(object):
    pass

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    
    archs=opt.archive

    if len(args)==0:
        print('Missing PV names')
        defer.returnValue(0)
    
    T0, Tend = makeTimeInterval(opt.start, opt.end)
    TT0, TT1 = timeTuple(T0), timeTuple(Tend)

    count = opt.count if opt.count>0 else conf.getint('defaultcount')

    h5file, _, path = opt.h5file.partition(':')
    if path=='':
        path='/'
    
    F = h5py.File(h5file, 'a')
    
    pvgroup = F.require_group(path)
    
    Chk = opt.chunk
    
    Ds = [None]*len(args)

    for i,pv in enumerate(args):
        pvstore = pvgroup.require_group(pv)

        # store complete time range covering all *requests*
        aT0 = tuple(pvstore.attrs.get('T0', ()))
        try:
            if aT0 is None or TT0 < aT0:
                pvstore.attrs['T0'] = TT0
        except TypeError:
            pvstore.attrs['T0'] = TT0

        aT1 = tuple(pvstore.attrs.get('T1', ()))
        try:
            if aT1 is None or TT1 < aT1:
                pvstore.attrs['T1'] = TT1
        except TypeError:
            pvstore.attrs['T1'] = TT1

        P = printInfo()
        P.file = F
        P.pvstore = pvstore
        P.pv=pv
        
        P.metaset = pvstore.get('meta')

        if P.metaset is None:
            P.metaset = pvstore.create_dataset('meta', shape=(0,),
                                               dtype=dbr_time,
                                               maxshape=(None,),
                                               chunks=(Chk,),
                                               shuffle=True,
                                               compression='gzip')

        P.valset = None

        print(pv)
        D = archive.fetchraw(pv, printData, archs=archs,
                                   cbArgs=(archive, P),
                                   T0=T0, Tend=Tend,
                                   count=count, chunkSize=Chk,
                                   enumAsInt=opt.enumAsInt)

        @D.addCallback
        def show(C, pv=pv):
            _log.info('%s received %s points', pv,C)

        Ds[i] = D

    yield defer.DeferredList(Ds, fireOnOneErrback=True)

    defer.returnValue(0)

def mangleArgs(opt, args):
    if len(args)==0:
        raise RuntimeError('Missing HDF5 file name')
    opt.h5file = args[0]
    return opt, args[1:]
