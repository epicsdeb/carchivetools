# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger("argrep")

from twisted.internet import defer

@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    if len(args)==0:
        args=['.*']

    _log.debug('Searching for: %s',args)

    archs=opt.archive
    _log.info('Looking in: %s',', '.join(archs))

    res = {}

    for pat in args:
        S = yield archive.search(pattern=pat, archs=archs,
                                 breakDown=opt.verbose>1)
        res.update(S)

    _log.debug('Found %d results',len(res))

    chs=res.keys()
    chs.sort()
    for c in chs:
        if opt.verbose==0:
            print c
            continue

        ranges=res[c]
        if opt.verbose>1:
            print c
            for s,e,ar in ranges:
                print ' ', s, ',', e, ',', archive.lookupArchive(ar)

        elif opt.verbose>0:
            print ' ',ranges[0],',',ranges[1],',',c
