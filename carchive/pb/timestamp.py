import datetime
import math

def carchive_to_aapb(input_sec, input_nsec):
    '''Converts a Channel Archiver timestamp to a datetime and an Archiver Appliance PB timestamp.
    
    Channel Archiver: (secs, nano) - time since 1970 UTC
    Archiver Appliance PB: (year, seconds, nano) - time since year UTC
    '''
    
    input_dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=input_sec, microseconds=input_nsec/1000.0)
    
    year_dt = datetime.datetime(input_dt.year, 1, 1)
    
    into_year_delta = input_dt - year_dt
    
    into_year_sec_float = into_year_delta.total_seconds()
    
    f, i = math.modf(into_year_sec_float)
    
    into_year_sec = int(i)
    into_year_nsec = min(999999999, int(1e9 * f)) # TBD loss of precision
    
    return (input_dt, into_year_sec, into_year_nsec)
