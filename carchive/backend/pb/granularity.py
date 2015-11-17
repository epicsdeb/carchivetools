"""
This software is Copyright by the
 Board of Trustees of Michigan
 State University (c) Copyright 2015.
"""
from __future__ import absolute_import
import datetime
import calendar

'''
    Defines the types of granularity that can be used for exporting the data. 
    Granularity defines the names of the files that are used as well as which
    data (according to the timestamp) goes to which file.
'''

def get_granularity(gran_str):
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

class YearSegment(object):
    def __init__(self, year):
        self.year = year
    
    def start_time(self):
        return datetime.datetime(self.year, 1, 1)
    
    def next_segment(self):
        return YearSegment(self.year + 1)
    
    def file_suffix(self):
        return '{0:04}'.format(self.year)

class YearGranularity(object):
    def get_segment_for_time(self, time):
        return YearSegment(time.year)
    
    def suffix_count(self):
        return 1

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
        return '{0}_{1:02}'.format(self.year_seg.file_suffix(), self.month)

class MonthGranularity(object):
    def get_segment_for_time(self, time):
        return MonthSegment(YearGranularity().get_segment_for_time(time), time.month)
    
    def suffix_count(self):
        return YearGranularity().suffix_count() + 1

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
        return '{0}_{1:02}'.format(self.month_seg.file_suffix(), self.day)

class DayGranularity(object):
    def get_segment_for_time(self, time):
        return DaySegment(MonthGranularity().get_segment_for_time(time), time.day)
    
    def suffix_count(self):
        return MonthGranularity().suffix_count() + 1

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
        return '{0}_{1:02}'.format(self.day_seg.file_suffix(), self.hour)

class HourGranularity(object):
    def get_segment_for_time(self, time):
        return HourSegment(DayGranularity().get_segment_for_time(time), time.hour)
    
    def suffix_count(self):
        return DayGranularity().suffix_count() + 1

class MinuteSegment(object):
    def __init__(self, minutes_step, hour_seg, minute):
        self.minutes_step = minutes_step
        self.minute = (minute / minutes_step)*minutes_step
        self.hour_seg = hour_seg
    
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
        return '{0}_{1:02}'.format(self.hour_seg.file_suffix(), self.minute)

class MinuteGranularity(object):
    def __init__(self, minutes_step):
        self.minutes_step = minutes_step
    
    def get_segment_for_time(self, time):
        return MinuteSegment(self.minutes_step, HourGranularity().get_segment_for_time(time), time.minute)
    
    def suffix_count(self):
        return HourGranularity().suffix_count() + 1
