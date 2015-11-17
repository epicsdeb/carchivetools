from __future__ import absolute_import
import datetime
from carchive.date import timeTuple

'''
This software is Copyright by the
 Board of Trustees of Michigan
 State University (c) Copyright 2015.

    Timestamps:
        Python datetime: Gregorian calendar
        Channel Archiver: (secs, nano) - time since 1970 UTC
        Archiver Appliance PB: (year, seconds, nano) - time since year UTC
    
    Note: we don't have the CA-->PB conversion function here which is the most
    important thing. That conversion is done without a helper function, in an
    exact manner.
'''

def dt_to_carchive(input_dt):
    '''Converts a datetime to Channel Archiver timestamp.
    This is exact. The input format has microsecond resolution,
    but the output format has nanosecond.'''
        
    #delta = input_dt - datetime.datetime(1970, 1, 1)
    #seconds = delta.seconds + delta.days * 24 * 3600
    #nanoseconds = delta.microseconds * 1000
    #return (seconds, nanoseconds)
    
    ''' This approach works for the local time '''
    return timeTuple(input_dt)

def pb_to_dt(year, secondsintoyear, nano):
    return datetime.datetime(year, 1, 1) + datetime.timedelta(seconds=secondsintoyear, microseconds=nano/1000.0)
