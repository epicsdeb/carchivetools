# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

import logging
_log = logging.getLogger("arget")

from twisted.internet import defer

from carchive.date import makeTimeInterval, makeTime

class Printer(object):
    def __init__(self, opt, pvname):
        self.pvname, self.first = pvname, True
        self.skipFirst, self.printName = opt.skipFirst, False
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
        if self.first and self.skipFirst:
            self.first=False
            data, meta = data[1:,:], meta[1:]
            if len(meta)==0:
                return # no data
        if not self.printName:
            self.printName=True
            print(self.pvname)

        if data.shape[1] == 1: # scalar
            data = data.reshape((data.shape[0],))
            for M,D in zip(meta,data):
                print(self.timefmt((M['sec'],int(M['ns']))), D, archive.severity(M['severity']), archive.status(M['status']))

        else: # waveform
            for i,M in enumerate(meta):
                print(self.timefmt((M['sec'],int(M['ns']))), archive.severity(M['severity']), archive.status(M['status']), ', '.join(map(str,data[i,:].tolist())))

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, breakDown=None, **kws):
    if opt.how=='raw':
        op = archive.fetchraw
    elif opt.how=='plot':
        op = archive.fetchplot
    else:
        raise ValueError('Unknown plot type %s'%opt.how)

    archs=opt.archive

    if len(args)==0:
        print('Missing PV names')
        defer.returnValue(0)
    
    T0, Tend = makeTimeInterval(opt.start, opt.end)

    _log.debug("Time range: %s -> %s", T0, Tend)

    count = opt.count if opt.count>0 else conf.getint('defaultcount')

    for pv in args:
        printData = Printer(opt, pv)
        D = yield op(pv, printData, archs=archs,
                     cbArgs=(archive,),
                     T0=T0, Tend=Tend, breakDown=breakDown,
                     count=count, chunkSize=opt.chunk,
                     enumAsInt=opt.enumAsInt)

        C = yield D
        if printData.printName:
            print('Found %s points'%C)
