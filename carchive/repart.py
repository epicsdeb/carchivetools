# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.

Archiver Appliance PB file re-partitioning

Reads samples from a set of .pb files
and writes out a set of .pb files with the requested partion granularity
"""
from __future__ import absolute_import

import logging
_log = logging.getLogger(__name__)
import datetime, calendar

from .backend import EPICSEvent_pb2 as pb
from .backend.pbdecode import unescape, escape

_fields = {
    0:pb.ScalarString,
    1:pb.ScalarShort,
    2:pb.ScalarFloat,
    3:pb.ScalarEnum,
    4:pb.ScalarByte,
    5:pb.ScalarInt,
    6:pb.ScalarDouble,
    7:pb.VectorString,
    8:pb.VectorShort,
    9:pb.VectorFloat,
    10:pb.VectorEnum,
    #11:pb.VectorByte, # missing?
    12:pb.VectorInt,
    13:pb.VectorDouble,
    14:pb.V4GenericBytes,
}

class PartitionBase(object):
    def __init__(self, header, firstsamp):
        self.firstsamp = firstsamp
        self.year = header.year
        Y = self._year_sec = calendar.timegm(datetime.date(header.year,1,1).timetuple())
        S = firstsamp.secondsintoyear
        self._sec = Y+S
        self._dt = datetime.datetime.utcfromtimestamp(Y+S)
    def _after(self):
        _log.debug("using %s %d (%d)", self._dt, self.firstsamp.secondsintoyear, self._year_sec)
        _log.debug("Partition start %s", self.first)
        _log.debug("Partition end %s", self.last)
        self.first = calendar.timegm(self.first.timetuple()) - self._year_sec
        self.last = calendar.timegm(self.last.timetuple()) - self._year_sec
        _log.info('Start partition: %d %d %.02f days', self.first, self.last,
                  (self.last-self.first)/86400.)
        assert isinstance(self.suffix,str)
        assert self.first<self.last, (self.first, self.last)
        assert self.first<=self.firstsamp.secondsintoyear, (self.first, self.firstsamp.secondsintoyear)
        assert self.firstsamp.secondsintoyear<self.last, (self.firstsamp.secondsintoyear, self.last)

class YearPartition(PartitionBase):
    def __init__(self, header, firstsamp):
        super(YearPartition,self).__init__(header, firstsamp)
        DT = self._dt
        self.first = datetime.datetime(DT.year, 1, 1)
        self.last = datetime.datetime(DT.year+1, 1, 1)
        self.suffix = '%d'%(DT.year,)
        self._after()

class MonthPartion(PartitionBase):
    def __init__(self, header, firstsamp):
        super(MonthPartion,self).__init__(header, firstsamp)
        DT = self._dt
        self.first = datetime.datetime(DT.year, DT.month, 1)
        if DT.month<12:
            self.last = datetime.datetime(DT.year, DT.month+1, 1)
        else:
            self.last = datetime.datetime(DT.year+1, 1, 1)
        self.suffix = '%d_%02d'%(DT.year, DT.month)
        self._after()

class DayPartion(PartitionBase):
    def __init__(self, header, firstsamp):
        super(DayPartion,self).__init__(header, firstsamp)
        DT = self._dt
        self.first = datetime.datetime(DT.year, DT.month, DT.day)
        self.last = self.first+datetime.timedelta(days=1)
        self.suffix = '%d_%02d_%02d'%(DT.year, DT.month, DT.day)
        self._after()

_partitions = {
    'year':YearPartition,
    'month':MonthPartion,
    'day':DayPartion,
}

def args():
    import argparse
    P=argparse.ArgumentParser()
    P.add_argument('--prefix', default='./out:', help='Output file path prefix')
    P.add_argument('parttype', help='Output partition granularity')
    P.add_argument('srcfiles', type=argparse.FileType(mode='r'),
                   nargs='+', help='Input PB file(s)')
    return P.parse_args()

def main(args):
    cls = _partitions[args.parttype]
    part = None
    outfile = None
    outyear = None

    for src in args.srcfiles:
        _log.info('Reading: %s', src.name)
        header, sampdecode = None, None
        for rawL in src:
            L = unescape(rawL[:-1])
            if not rawL:
                # section boundary
                _log.info('Input section boundary')
                header, sampdecode = None, None

            elif header is None:
                # Start of section
                header = pb.PayloadInfo()
                try:
                    header.ParseFromString(L)
                    headerRaw = L
                except:
                    _log.error('Error decoding: %s', repr(L))
                    raise
                sampdecode = _fields[header.type]
                _log.info('Input header: %d %d %s',
                          header.type, header.year, header.pvname)

                if outfile is not None and outyear!=header.year:
                    _log.info('End output partition at year boundary')
                    outfile.close()
                    outfile, part = None, None

            else:
                # Sample
                S = sampdecode()
                try:
                    S.ParseFromString(L)
                    #print S.secondsintoyear
                except:
                    _log.error('Error decoding: %s', repr(L))
                    raise
                if part is not None:
                    if S.secondsintoyear>=part.last:
                        # passed end of partition
                        _log.info('End output partition: %d %d', S.secondsintoyear, part.last)
                        outfile.close()
                        outfile, part = None, None

                if part is None:
                    part = cls(header, S)
                    outname = args.prefix+part.suffix+".pb"
                    outyear = header.year
                    outfile = open(outname, "w")
                    outfile.write(escape(headerRaw)+'\n')
                    _log.info('Writing: %s', outname)

                outfile.write(rawL)

    _log.info('End last output partition')
    outfile.close()

if __name__=='__main__':
    args = args()
    logging.basicConfig(level=logging.DEBUG)
    main(args)
