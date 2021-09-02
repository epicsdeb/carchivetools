# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

import logging
_log = logging.getLogger("argrep")

from twisted.internet import defer
from ..date import makeTime

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, breakDown=None, **kws):
    if len(args)==0:
        args=['.*']

    archs=opt.archive
    _log.info('Looking in: %s',', '.join(map(str,archs)))

    res = breakDown

    _log.debug('Found %d results',len(res))

    chs=list(res.keys())
    chs.sort()
    for c in chs:
        print(c)
        if opt.verbose>0:

            ranges=res[c]
            for s,e,ar in ranges:
                print(' ', makeTime(s), ',', makeTime(e), ',', archive.lookupArchive(ar))

    yield defer.succeed(None)
