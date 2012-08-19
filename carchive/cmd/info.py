# -*- coding: utf-8 -*-

import sys

def cmd(archive=None, opt=None, conf=None, **kws):
    if opt.verbose>0:
        conf.write(sys.stdout)
        sys.stdout.write('\n')
        print archive

    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)

    print 'Archives:'
    archs.sort()
    for ar in archs:
        print ' ',ar
