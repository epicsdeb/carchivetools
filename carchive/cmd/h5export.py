# -*- coding: utf-8 -*-

import h5py

import numpy as np

from twisted.internet import defer

from carchive.date import makeTimeInterval
from carchive.archive import dbr_time

def printData(data, meta, archive, info):
    print 'shape',data.shape
    metaset = info.metaset
    
    if not info.valset: # first data
        pvstore = info.pvstore
        valset = pvstore.get('value')
        if valset is None:
            valset = pvstore.create_dataset('value',
                                            shape = (0,0),
                                            dtype=data.dtype,
                                            maxshape=(None,None),
                                            chunks=data.shape)
        info.valset = valset
    else: # additional data
        valset = info.valset

    if metaset.shape[0]:
        lastsamp = (metaset['sec'][-1], metaset['ns'][-1])
        print 'L',lastsamp
        newstart = (meta['sec'][0], meta['ns'][0])  
        
        if(lastsamp >= newstart):
            print 'Ignoring overlapping data'
            return  

    mstart = info.metaset.shape[0]
    info.metaset.resize((mstart+meta.shape[0],))
    
    info.metaset[mstart:] = meta
    
    start = valset.shape[0]
    
    shape = (valset.shape[0] + data.shape[0],
             max(valset.shape[1], data.shape[1]))

    valset.resize(shape)

    valset[start:,:data.shape[1]] = data

    print '>',valset.shape[0]

class printInfo(object):
    pass

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    
    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)

    if len(args)==0:
        print 'Missing HDF5 file name'
        defer.returnValue(0)
    elif len(args)==1:
        print 'Missing PV names'
        defer.returnValue(0)
    
    T0, Tend = makeTimeInterval(opt.start, opt.end)
    count = opt.count if opt.count>0 else None
    
    h5file, _, path = args.pop(0).partition(':')
    if path=='':
        path='/'
    
    F = h5py.File(h5file, 'a')
    
    pvgroup = F.require_group(path)
    
    Chk = 1000

    for pv in args:
        pvstore = pvgroup.require_group(pv)
        
        P = printInfo()
        P.file = F
        P.pvstore = pvstore
        
        P.metaset = pvstore.get('meta')

        if P.metaset is None:
            P.metaset = pvstore.create_dataset('meta', shape=(0,),
                                               dtype=dbr_time,
                                               maxshape=(None,),
                                               chunks=(Chk,))

        P.valset = None

        print pv
        D = yield archive.fetchraw(pv, printData, archs=archs,
                                   cbArgs=(archive, P),
                                   T0=T0, Tend=Tend,
                                   count=count, chunkSize=Chk)

        C = yield D
        print 'Found %d points'%C
