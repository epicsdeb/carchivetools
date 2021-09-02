# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

import sys

def cmd(archive=None, opt=None, conf=None, **kws):
    if opt.verbose>1:
        conf.write(sys.stdout)
        sys.stdout.write('\n')
        print archive

    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)

    if opt.verbose>0:
      print('Archives:')
    archs.sort()
    for ar in archs:
        print(ar)
