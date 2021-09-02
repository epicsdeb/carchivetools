# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

import logging
_log = logging.getLogger("arsnap")

from twisted.internet import defer

from carchive.date import makeTimeInterval, makeTime

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    
    if not opt.timefmt or opt.timefmt=='string':
        def timefmt(ts):
            return makeTime(ts)
    elif opt.timefmt=='posix':
        def timefmt((sec,ns)):
            return sec+1e-9*ns
    else:
        raise ValueError("Invalid time format %s"%opt.timefmt)

    if len(args)==0:
        print('Missing PV names')
        defer.returnValue(0)

    T = makeTimeInterval(opt.start, None)[0]

    _log.debug("Time: %s", T)

    archs=opt.archive

    vals, metas = yield archive.fetchsnap(args, archs=archs,
                     T=T, chunkSize=opt.chunk,
                     enumAsInt=opt.enumAsInt)

    for n, data, M in zip(args, vals, metas):
        print(n,'\t', timefmt((M['sec'],int(M['ns']))), end=None)
        try:
            scalar = len(data)>1
        except:
            scalar=True
        if scalar:
            print(data, archive.severity(M['severity']), archive.status(M['status']))

        else: # waveform
            print(archive.severity(M['severity']), archive.status(M['status']), ', '.join(map(str,data)))
