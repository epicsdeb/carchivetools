#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys, os.path, logging
from optparse import OptionParser

pname=os.path.basename(sys.argv[0])

act=None
if pname.startswith('arinfo'):
    act='info'
elif pname.startswith('argrep'):
    act='grep'
elif pname.startswith('arget'):
    act='get'

par=OptionParser(
    usage='%prog [options] channel <channels ...>',
    description='Query the channel archiver'
    )

par.add_option('-I','--info', action="store_true", default=False,
               help='Show archive server information')
par.add_option('-S','--search', action="store_true", default=False,
               help='Search for channels matching the given pattern(s)')
par.add_option('-G','--get', action="store_true", default=False,
               help='Retrieve data for given channels')

par.add_option('-u','--url', metavar='NAME or URL',
               help='Either a config key, host name, or full url for the server')
par.add_option('-s','--start', metavar='TIME',
               help='Start of query window (required)')
par.add_option('-e','--end', metavar='TIME',
               help='End of query window (defaults to current system time)')
par.add_option('-c','--count', metavar='NUM', default=10, type="int",
               help='Maximum number of samples to read')
par.add_option('-a','--archive', metavar='NAME', action='append', default=[],
               help='Archive name.  Wildcards allowed.  Can be given more than once')
par.add_option('-H','--how', metavar='NAME', default='raw',
               help="Query method (eg. raw)")

par.add_option('-M','--merge', metavar='NAME', default='simple',
               help='How to attempt to combine data for one channel received in '
               'different responces.  Options: none, simple')

par.add_option('-v','--verbose', action='count', default=0,
               help='Print more')
par.add_option('-d','--debug', action='store_true', default=False,
               help='Show archiver queries')

opt, args = par.parse_args()

if opt.merge not in ['none','simple']:
    par.error('Invalid merge method %s'%opt.merge)

if opt.info:
    act='info'
elif opt.search:
    act='grep'
elif opt.get:
    act='get'

LVL={0:logging.WARN, 1:logging.INFO, 2:logging.DEBUG}

logging.basicConfig(format='%(message)s',level=LVL.get(opt.verbose, LVL[2]))

from carchive import Archiver
from carchive.date import makeTime
from carchive._conf import _conf as conf
from carchive import data

if opt.url:
    serv=Archiver(opt.url, debug=opt.debug)
elif conf.has_option('_unspecified_','defaulthost'):
    serv=Archiver(conf.get('_unspecified_','defaulthost'), debug=opt.debug)
else:
    par.error('Unable to determine which archive server to use')

if len(opt.archive)==0:
    if conf.has_section(opt.url):
        opt.archive=[conf.get(opt.url, 'defaultarchs')]
    else:
        opt.archive=[conf.get('_unspecified_', 'defaultarchs')]

archs=set()
for ar in opt.archive:
    archs|=set(serv.archs(pattern=ar))
archs=list(archs)

if act=='info':
    if opt.verbose>0:
        conf.write(sys.stdout)
        sys.stdout.write('\n')
        print serv

    print 'Archives:'
    archs.sort()
    for ar in archs:
        print ' ',ar
    sys.exit(0)

elif act=='grep':
    if len(args)==0:
        args=['.*']

    res=serv.search(args[0], archs=archs)

    for pat in args[1:]:
        for ch, ranges in serv.search(args[1], archs=archs).iteritems():
            for r in ranges:
                if r in res[ch]:
                    continue
                res[ch].append(r)
            res[ch].sort(key=lambda r:r[0])

    chs=res.keys()
    chs.sort()
    for c in chs:
        ranges=res[c]
        if opt.verbose>1:
            print c
            for s,e,ar in ranges:
                print ' ', makeTime(s),
                print makeTime(e),
                print ar

        elif opt.verbose>0:
            print ' ',makeTime(ranges[0][0]),
            print ' ',makeTime(ranges[-1][1]),
            print c

        else:
            print c

elif act=='get':
    if len(args)==0:
        par.error('No channel to query')
    elif opt.how not in serv.how:
        par.error("How %s is invalid.  Use one of %s"%(opt.how, ', '.join(serv.how)))
    elif not opt.start:
        par.error("Start time is required")

    Q=serv.Q().set(names=args, patterns=True, archs=archs)
    Q.how=opt.how
    Q.count=opt.count
    Q.start=opt.start
    if opt.end:
        Q.end=opt.end

    for ch, ranges in Q.execute().iteritems():
        print ch
        if opt.merge=='simple':
            ranges=data.simpleMerge(ranges, Q.start, Q.end)
        for d in ranges:
            print '==='
            d.pPrint()

else:
    par.error('Unkown action: %s'%act)
