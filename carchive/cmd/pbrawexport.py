# -*- coding: utf-8 -*-

from __future__ import print_function
from twisted.internet import defer
from carchive.date import makeTimeInterval, makeTime
from carchive.pb import EPICSEvent_pb2

class Printer(object):
    def __init__(self, opt):
        if not opt.timefmt or opt.timefmt=='string':
            self.timefmt = self.timestring
        elif opt.timefmt=='posix':
            self.timefmt = self.timeposix
        else:
            raise ValueError("Invalid time format %s"%opt.timefmt)

    def timeposix(self, (sec,ns)):
        return sec+1e-9*ns

    def timestring(self, ts):
        return makeTime(ts)

    def __call__(self, data, meta, archive):
        assert len(meta)==data.shape[0]

        if data.shape[1] == 1: # scalar
            data = data.reshape((data.shape[0],))
            for M,D in zip(meta,data):
                print('{} {} {} {}'.format(self.timefmt((M['sec'],int(M['ns']))), D, archive.severity(M['severity']), archive.status(M['status'])))

        else: # waveform
            for i,M in enumerate(meta):
                print('{} {} {} {}'.format(self.timefmt((M['sec'],int(M['ns']))), archive.severity(M['severity']), archive.status(M['status']), data[i,:].tolist()))

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)
    
    # Collect PV name patterns.
    patterns = []
    if opt.export_all:
        patterns.append('.*')
    if opt.export_pattern is not None:
        patterns += opt.export_pattern
    
    # Collect PVs to archive...
    pvs = set()
    
    # Query PVs for patterns.
    for pattern in patterns:
        print('Querying pattern: {}'.format(pattern))
        search_result = yield archive.search(pattern=pattern, archs=archs, breakDown=opt.verbose>1)
        print('--> {} PVs.'.format(len(search_result)))
        pvs.update(search_result)

    # Add explicit PVs.
    pvs.update(args)
    
    # Sort PVs.
    pvs = sorted(pvs)
    
    # Check we have any PVs.
    if len(pvs)==0:
        print('Missing PV names')
        defer.returnValue(0)
    
    # Resolve time interval.
    T0, Tend = makeTimeInterval(opt.start, opt.end)

    # Print some info.
    print('Time range: {} -> {}'.format(T0, Tend))
    print('PVs: {}'.format(' '.join(pvs)))

    count = opt.count if opt.count>0 else conf.getint('defaultcount')
    
    printData = Printer(opt)
    
    # Archive PVs one by one.
    for pv in pvs:
        print('Archiving PV: {}'.format(pv))
        
        D = yield archive.fetchraw(pv, printData, archs=archs,
                                   cbArgs=(archive,),
                                   T0=T0, Tend=Tend,
                                   count=count, chunkSize=opt.chunk,
                                   enumAsInt=opt.enumAsInt)

        sample_count = yield D
        print('--> {} samples.'.format(sample_count))
    
    defer.returnValue(0)
