# -*- coding: utf-8 -*-

from twisted.internet import defer

from carchive.date import makeTimeInterval, makeTime

def printData(data, meta, archive):
    assert len(meta)==data.shape[0]
    
    if data.shape[1] == 1: # scalar
        data = data.reshape((data.shape[0],))
        for M,D in zip(meta,data):
            print makeTime((M['sec'],int(M['ns']))), D,
            print archive.severity(M['severity']),
            print archive.status(M['status'])

    else: # waveform
        for i,M in enumerate(meta):
            print makeTime((M['sec'],int(M['ns']))),
            print archive.severity(M['severity']),
            print archive.status(M['status']),
            print data[i,:]

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    
    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)

    if len(args)==0:
        print 'Missing PV names'
        defer.returnValue(0)
    
    T0, Tend = makeTimeInterval(opt.start, opt.end)
    count = opt.count if opt.count>0 else None

    for pv in args:
        print pv
        D = yield archive.fetchraw(pv, printData, archs=archs,
                                   cbArgs=(archive,),
                                   T0=T0, Tend=Tend,
                                   count=count, chunkSize=1000)

        C = yield D
        print 'Found %d points'%C
