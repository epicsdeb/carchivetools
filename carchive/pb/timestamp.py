import datetime
import math

'''
    Timestamps:
        Python datetime: Gregorian calendar
        Channel Archiver: (secs, nano) - time since 1970 UTC
        Archiver Appliance PB: (year, seconds, nano) - time since year UTC
    
    Note: we don't have the CA-->PB conversion function here which is the most
    important thing. That conversion is done without a helper function, in an
    exact manner.
'''

def dt_to_carchive(input_dt):
    delta = input_dt - datetime.datetime(1970, 1, 1)
    f, i = math.modf(delta.total_seconds())
    sec = int(i)
    nsec = min(999999999, int(1e9 * f)) # TBD loss of precision
    return (sec, nsec)

def pb_to_dt(year, secondsintoyear, nano):
    return datetime.datetime(year, 1, 1) + datetime.timedelta(seconds=secondsintoyear, microseconds=nano/1000.0)
