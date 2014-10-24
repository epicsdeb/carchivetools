# -*- coding: utf-8 -*-

from __future__ import print_function
import re
import datetime
import calendar
import os
from twisted.internet import defer
from carchive.date import makeTimeInterval, makeTime
from carchive.pb import EPICSEvent_pb2


# Escaping...
# Archiver PB files are split by lines. The first line is some header information.
# The remaining lines are samples. In these samples some characters are escaped.
# The escaping rules were obtained from LineEscaper.java.

PB_ESCAPE_MAP = {
    '\x1B': '\x1B\x01',
    '\x0A': '\x1B\x02',
    '\x0D': '\x1B\x03',
}

def escape_newlines_for_pb(data):
    return ''.join(PB_ESCAPE_MAP[c] if c in PB_ESCAPE_MAP else c for c in data)


# File location mapping...

def file_path_for_suffix(out_dir, delimiters, pv_name, time_suffix):
    regexPattern = '|'.join(map(re.escape, delimiters))
    path_components = [out_dir] + re.split(regexPattern, pv_name)
    path_components[-1] += ':{}.pb'.format(time_suffix)
    return os.path.join(*path_components)


# Granularity...

class YearSegment(object):
    def __init__(self, year):
        self.year = year
    
    def start_time(self):
        return datetime.datetime(self.year, 1, 1)
    
    def next_segment(self):
        return YearSegment(self.year + 1)
    
    def file_suffix(self):
        return '{:04}'.format(self.year)

class YearGranularity(object):
    def get_segment_for_time(self, time):
        return YearSegment(time.year)

class MonthSegment(object):
    def __init__(self, year_seg, month):
        self.year_seg = year_seg
        self.month = month
        self.days_in_month = calendar.monthrange(year_seg.year, month)[1]
    
    def start_time(self):
        return datetime.datetime(self.year_seg.year, self.month, 1)
    
    def next_segment(self):
        year_seg = self.year_seg
        month = self.month + 1
        if month == 13:
            year_seg = year_seg.next_segment()
            month = 1
        return MonthSegment(year_seg, month)
    
    def file_suffix(self):
        return '{}_{:02}'.format(self.year_seg.file_suffix(), self.month)

class MonthGranularity(object):
    def get_segment_for_time(self, time):
        return MonthSegment(YearGranularity().get_segment_for_time(time), time.month)

class DaySegment(object):
    def __init__(self, month_seg, day):
        self.month_seg = month_seg
        self.day = day
    
    def start_time (self):
        return datetime.datetime(self.month_seg.year_seg.year, self.month_seg.month, self.day)
    
    def next_segment (self):
        month_seg = self.month_seg
        day = self.day + 1
        if day == month_seg.days_in_month + 1:
            month_seg = month_seg.next_segment()
            day = 1
        return DaySegment(month_seg, day)
    
    def file_suffix(self):
        return '{}_{:02}'.format(self.month_seg.file_suffix(), self.day)

class DayGranularity(object):
    def get_segment_for_time(self, time):
        return DaySegment(MonthGranularity().get_segment_for_time(time), time.day)

class HourSegment(object):
    def __init__(self, day_seg, hour):
        self.day_seg = day_seg
        self.hour = hour
    
    def start_time(self):
        return datetime.datetime(self.day_seg.month_seg.year_seg.year, self.day_seg.month_seg.month, self.day_seg.day, self.hour)
    
    def next_segment(self):
        day_seg = self.day_seg
        hour = self.hour + 1
        if hour == 24:
            day_seg = day_seg.next_segment()
            hour = 0
        return HourSegment(day_seg, hour)
    
    def file_suffix(self):
        return '{}_{:02}'.format(self.day_seg.file_suffix(), self.hour)

class HourGranularity(object):
    def get_segment_for_time(self, time):
        return HourSegment(DayGranularity().get_segment_for_time(time), time.hour)

class MinuteSegment(object):
    def __init__(self, minutes_step, hour_seg, minute):
        self.minutes_step = minutes_step
        self.hour_seg = hour_seg
        self.minute = minute
    
    def start_time(self):
        return datetime.datetime(self.hour_seg.day_seg.month_seg.year_seg.year, self.hour_seg.day_seg.month_seg.month, self.hour_seg.day_seg.day, self.hour_seg.hour, self.minute)
    
    def next_segment(self):
        hour_seg = self.hour_seg
        minute = self.minute + self.minutes_step
        if minute >= 60:
            hour_seg = hour_seg.next_segment()
            minute -= 60
        return MinuteSegment(self.minutes_step, hour_seg, minute)
    
    def file_suffix(self):
        return '{}_{:02}'.format(self.hour_seg.file_suffix(), self.minute)

class MinuteGranularity(object):
    def __init__(self, minutes_step):
        self.minutes_step = minutes_step
    
    def get_segment_for_time(self, time):
        return MinuteSegment(self.minutes_step, HourGranularity().get_segment_for_time(time), time.minute)

def resolve_granularity(gran_str):
    if gran_str == '1year':
        return YearGranularity()
    if gran_str == '1month':
        return MonthGranularity()
    if gran_str == '1day':
        return DayGranularity()
    if gran_str == '1hour':
        return HourGranularity()
    if gran_str == '30min':
        return MinuteGranularity(30)
    if gran_str == '15min':
        return MinuteGranularity(15)
    if gran_str == '5min':
        return MinuteGranularity(5)
    return None


@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)
    
    # Get out dir.
    if opt.export_out_dir is None:
        print('Output directory not specified!')
        defer.returnValue(1)
    out_dir = opt.export_out_dir
    
    # Get granularity.
    if opt.export_granularity is None:
        print('Export granularity not specified!')
        defer.returnValue(1)
    gran = resolve_granularity(opt.export_granularity)
    if gran is None:
        print('Export granularity is not understood!')
        defer.returnValue(1)
    
    # Collect PV name delimiters.
    delimiters = ([] if opt.export_no_default_delimiters else [':', '-']) + \
        ([] if opt.export_delimiter is None else opt.export_delimiter)
    
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
        print('Have no PV names to archive!')
        defer.returnValue(1)
    
    # Resolve time interval.
    T0, Tend = makeTimeInterval(opt.start, opt.end)
    
    # Print some info.
    print('Time range: {} -> {}'.format(T0, Tend))
    print('PVs: {}'.format(' '.join(pvs)))
    
    # Archive PVs one by one.
    for pv in pvs:
        print('PV: {}'.format(pv))
        
        # Get the segment where the start time falls.
        segment = gran.get_segment_for_time(T0)
        
        while True:
            # Calculate the next segment.
            next_segment = segment.next_segment()
            
            # Get the time interval for this segment.
            segment_start_time = segment.start_time()
            segment_end_time = next_segment.start_time()
            
            # Stop if we've already covered the desired time interval.
            if segment_start_time >= Tend:
                break
            
            # Don't query outside the desired interval...
            query_start_time = max(segment_start_time, T0)
            query_end_time = min(segment_end_time, Tend)
            
            # Determine the path of the output file.
            out_file_path = file_path_for_suffix(out_dir, delimiters, pv, segment.file_suffix())
            
            print('[ {} - {} ) --> {}'.format(query_start_time, query_end_time, out_file_path))
            
            # Make sure the file doesn't already exist. There's a race but whatever.
            if os.path.isfile(out_file_path):
                print('Output file already exists!')
                defer.returnValue(1)
            
            # Open the file for writing.
            with open(out_file_path, 'wb') as file_handle:
                
                # This will be called for every chunk of samples.
                def data_cb(data, meta):
                    print('chunk')
                    is_waveform = data.shape[1] != 1
                    for (i, m) in enumerate(meta):
                        value = data[i] if is_waveform else data[i][0]
                        #print('{}'.format(value))
                
                # Ask for samples for this interval.
                # TBD check if archive.fetchraw interprets T0/Tend as [T0, Tend) as is the assumption here.
                segment_data = yield archive.fetchraw(pv, data_cb, archs=archs, cbArgs=(), T0=query_start_time, Tend=query_end_time, chunkSize=opt.chunk, enumAsInt=opt.enumAsInt)
                
                # Process these samples.
                sample_count = yield segment_data
            
            # Continue with the next segment.
            segment = next_segment
    
    print('All done.')
    
    defer.returnValue(0)
