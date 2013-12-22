# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger("argrep")

from twisted.internet import defer

from carchive.date import makeTimeInterval, makeTime

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
                print self.timefmt((M['sec'],int(M['ns']))), D,
                print archive.severity(M['severity']),
                print archive.status(M['status'])

        else: # waveform
            for i,M in enumerate(meta):
                print self.timefmt((M['sec'],int(M['ns']))),
                print archive.severity(M['severity']),
                print archive.status(M['status']),
                print data[i,:].tolist()

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):

    printData = Printer(opt)

    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)

    if len(args)==0:
        print 'Missing PV names'
        defer.returnValue(0)
    
    T0, Tend = makeTimeInterval(opt.start, opt.end)

    _log.debug("Time range: %s -> %s", T0, Tend)

    sect = conf.get('DEFAULT', 'defaultarchive')

    count = opt.count if opt.count>0 else conf.getint(sect, 'defaultcount')

    for pv in args:
        print pv
        D = yield archive.fetchraw(pv, printData, archs=archs,
                                   cbArgs=(archive,),
                                   T0=T0, Tend=Tend,
                                   count=count, chunkSize=opt.chunk,
                                   enumAsInt=opt.enumAsInt)

        C = yield D
        print 'Found %d points'%C
