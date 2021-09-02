# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.

Date string format

Parsing of absolute and relative dates and times
into datetime.datetime or datetime.timedelta instances

Supported string syntax:

  Absolute:
    "day/month[/year] hour:min[:sec[.fraction]][Z]"
    "[year-]month-day hour:min[:sec[.fraction]][Z]"
    "hour:min[:sec[.fraction]][Z]"

  Relative:
    "now"
    "### UUU [### UUU ...]"

  where ### is a signed floating point number,
  and UUU is a unit string.

  Supported unit strings

  us
  ms
  s, sec, secs, second, seconds
  m, min, mins, minute, minutes
  h, hrs, hour, hours
  d, day, days
  w, week, weeks

  eg: "-1.4 week 2 hours"
"""

import datetime, time, re, sys, calendar
from collections import defaultdict

__all__ = ["makeTime", "makeTimeInterval", 'timeTuple', 'isoString']

# python provides no concrete implementations of tzinfo
# and even if the zone database is available (pytz module)
# there is no easy way to find the system timezone name
# in a format which pytz understands.
# I give up.  doctests will only work in US/Eastern

# A guide to time format manipulations in Python
# Formats: float, timetuple, and datetime
#  float is always posix, timetuple and datetime are TZ specific.
#  Timetuples track TZ implicitly.
#  datetime may have explicit TZ, defaults to None (implicit TZ)
#
# Conversions:
#   time.gmtime()     float -> timetuple UTC
#   time.localtime()  float -> timetuple Local
#   time.mktime()     timetuple Local -> float
#   calendar.timegm() timetuple UTC -> float
#
#   datetime.fromtimestamp()    float -> datetime Local
#   datetime.utcfromtimestamp() float -> datetime UTC
#   datetime.timetuple()        datetime Local -> timetuple Local
#   datetime.timetuple()        datetime UTC   -> timetuple UTC
#   datetime.utctimetuple()     datetime Local -> timetuple Local (Never call this)
#   datetime(*timetuple)        timetuple Local -> datetime Local
#   datetime(*timetuple)        timetuple UTC   -> datetime UTC
#
# Note: none of the functions producing datatime objects set the TZ
#

# Match: H:M[:S[.F]]
_tpat = r'(\d+):(\d+)(?::(\d+)(?:\.(\d+))?)?'

# Match D/M[/Y]
_dpat1= r'(\d+)/(\d+)(?:/(\d+))?'

# Match [Y-]M-D
_dpat2= r'(?:(\d+)-)?(\d+)-(\d+)?'

_tzpat= r'\s*([Zz])?'

# Match [D/M[/Y] ]H:M[:S[.F]][Z]
_ts1 = re.compile(r'(?:%s[\sT]+)?%s%s'%(_dpat1, _tpat, _tzpat))

# Match [[Y-]M-D ]H:M[:S[.F]][Z]
_ts2 = re.compile(r'(?:%s[\sT]+)?%s%s'%(_dpat2, _tpat, _tzpat))

# short hand and conversions for interval specifications
_units={
    'us':('microseconds',1),
    'ms':('microseconds',1000),
    's':('seconds',1),
    'sec':('seconds',1),
    'secs':('seconds',1),
    'm':('minutes',1),
    'min':('minutes',1),
    'mins':('minutes',1),
    'h':('hours',1),
    'hrs':('hours',1),
    'd':('days',1),
    'w':('days',7),
    'week':('days',7),
    'weeks':('days',7),
}

if sys.version_info<(2,7):
    def total_seconds(td):
        """Convert a timedelta to a number of seconds (float with us prec.)

        >>> TD = datetime.timedelta
        >>> total_seconds(TD(0,2)-TD(0,1))
        1.0
        >>> total_seconds(TD(2,2)-TD(1,1))
        86401.0
        >>> T=total_seconds(TD(2,2,2000)-TD(1,1,1000))
        >>> T>86401.000999 and T<86401.001001
        True
        """
        return td.days*86400.0 + td.seconds + td.microseconds*1e-6
else:
    total_seconds = datetime.timedelta.total_seconds

def _fromAbsString(intime, now):
    """Parse the given string into a tuple of (sec, ns) or None if the format is wrong.
    
    Year, Month, Day, seconds, and fraction may be omitted.
    
    'now' is a float time used to fill in missing YMD components, SF are zeroed
    
    >>> _fromAbsString('2014-12-1 17:38:43.903230001', 5)
    (1417473523, 903230001)
    >>> _fromAbsString('2014-12-01  22:38:43.903230Z', 5)
    (1417473523, 903230000)
    >>> _fromAbsString('2014-12-01  22:38:43.903230z', 5)
    (1417473523, 903230000)
    >>> _fromAbsString('2014-12-01  22:38:43 Z', 5)
    (1417473523, 0)
    >>> 1417473523-43
    1417473480
    >>> _fromAbsString('2014-12-01  22:38Z', 1417473523)
    (1417473480, 0)
    >>> _fromAbsString('12-01 22:38:43Z', 1417473523)
    (1417473523, 0)
    >>> _fromAbsString('22:38:43Z', 1417473523)
    (1417473523, 0)
    >>> _fromAbsString('22:38Z', 1417473523)
    (1417473480, 0)
    >>>
    >>> _fromAbsString('1/12/2014  22:38:43.903230Z', 5)
    (1417473523, 903230000)
    >>> _fromAbsString('1/12/2014  22:38:43Z', 1417473523)
    (1417473523, 0)
    >>> _fromAbsString('1/12  22:38Z', 1417473523)
    (1417473480, 0)
    >>> _fromAbsString('22:38Z', 1417473523)
    (1417473480, 0)
    >>>
    >>> _fromAbsString('2014-12-01T22:38:43.903230Z', 5)
    (1417473523, 903230000)
    >>>
    >>> _fromAbsString("-1 h", 5)
    >>> _fromAbsString("Invalid", 5)
    >>>
    """

    M = _ts1.match(intime)
    if M is not None:
        #day, mon, year, hour, min, sec, frac = M.group()
        parts = list(M.groups())
        parts[0], parts[2] = parts[2], parts[0] # swap year and day
    else:
        M = _ts2.match(intime)
        if M is not None:
            #year, mon, day, hour, min, sec, frac = M.group()
            parts = list(M.groups())
        else:
            return None

    # converter from timetuple to float
    conv = calendar.timegm if parts[7] in ['z','Z'] else time.mktime
    # converter from float to timetuple
    rconv = time.gmtime if parts[7] in ['z','Z'] else time.localtime

    D = rconv(now)[:3] + (0,0,0) # keep YMD, zero HMS

    nsec = 0.0
    if parts[6] is not None:
        nsec = float('0.'+parts[6])*1e9 # fraction to nanoseconds

    # if component is not None, convert to int(), otherwise use the default

    parts = [int(v) if v else d for v,d in zip(parts[:6], D)]
    parts += [0,0,-1] # disable DST compensation

    return int(conv(parts)), int(nsec)

def timeTuple(dt):
    """Convert (local) datetime object to (sec, nsec)

    *sec* is POSIX seconds

    >>> import datetime
    >>> now=datetime.datetime(2011, 3, 15, 13)
    >>> timeTuple(now)
    (1300208400, 0)
    >>> now=datetime.datetime(1970, 1, 1, 0, 1, 5)
    >>> timeTuple(now)
    (18065, 0)
    """
    S=int(time.mktime(dt.timetuple()))
    NS=dt.microsecond*1000
    return S,NS

def isoString(dt):
    """Convert a (local) datetime object to a ISO 8601 UTC string representation

    eg. 2014-04-10T16:27:37.767454Z

    >>> import datetime
    >>> now=datetime.datetime(2011, 3, 15, 13, 15, 14, 123000)
    >>> isoString(now)
    '2011-03-15T17:15:14.123000Z'
    """
    S, NS = timeTuple(dt)
    udt = datetime.datetime.utcfromtimestamp(S+NS*1e-9)
    return udt.isoformat('T')+'Z'

def makeTime(intime, now=None):
    """Turn *intime* into (local) datetime or timedelta

    *intime* can be a tuple (sec,nsec), number, or string

    *now* is a datetime.datetime used to fill in omitted
    parts of the time.  If None then datetime.datetime.now()
    is used.

    Note: Time may be specified to microsecond precision

    >>> import datetime
    >>> now=datetime.datetime(2011, 3, 15, 12)
    >>> now
    datetime.datetime(2011, 3, 15, 12, 0)
    >>> makeTime(now,now)==now
    True
    >>> makeTime(18065,now)
    datetime.datetime(1970, 1, 1, 0, 1, 5)
    >>>
    >>> makeTime(1300584688.9705319,now)
    datetime.datetime(2011, 3, 19, 21, 31, 28, 970531)
    >>> makeTime('1300584688.9705319',now)
    datetime.datetime(2011, 3, 19, 21, 31, 28, 970531)
    >>> makeTime( '12:01', now)
    datetime.datetime(2011, 3, 15, 12, 1)
    >>> makeTime( '12:01:14', now)
    datetime.datetime(2011, 3, 15, 12, 1, 14)
    >>> makeTime( '12:01:14.123456789', now)
    datetime.datetime(2011, 3, 15, 12, 1, 14, 123457)
    >>> makeTime( '12:01:14.123456389', now)
    datetime.datetime(2011, 3, 15, 12, 1, 14, 123456)
    >>> makeTime( '14/3 12:01', now)
    datetime.datetime(2011, 3, 14, 12, 1)
    >>> makeTime( '14/3 12:01Z', now)
    datetime.datetime(2011, 3, 14, 8, 1)
    >>> makeTime( '14/3 12:01:14', now)
    datetime.datetime(2011, 3, 14, 12, 1, 14)
    >>> makeTime( '14/3/2012 12:01:14.123456', now)
    datetime.datetime(2012, 3, 14, 12, 1, 14, 123456)
    >>>
    >>> makeTime('2014-12-01  22:38:43.903230Z', now)
    datetime.datetime(2014, 12, 1, 17, 38, 43, 903230)
    >>>
    >>> now=datetime.datetime.now()
    >>>
    >>> makeTime('-1 hours', now)
    datetime.timedelta(-1, 82800)
    >>> makeTime('-1 hours 5 minute', now)
    datetime.timedelta(-1, 83100)
    >>> makeTime('1 hours', now)
    datetime.timedelta(0, 3600)
    >>> makeTime('1 week', now)
    datetime.timedelta(7)
    >>> makeTime('0.25 week', now)
    datetime.timedelta(1, 64800)
    >>> makeTime('1 week -1 h', now)
    datetime.timedelta(6, 82800)
    >>> makeTime('1 week -1 h +1 m', now)
    datetime.timedelta(6, 82860)
    >>> makeTime('1 week -1 h +1 m -1 secs', now)
    datetime.timedelta(6, 82859)
    >>> makeTime('1 week -1 h +1 m -1.5 secs', now)
    datetime.timedelta(6, 82858, 500000)
    >>> makeTime('1 week -1 h +1 m -1 secs 1 ms', now)
    datetime.timedelta(6, 82859, 1000)
    >>> makeTime('1 week -1 h +1 m -1 secs 1 ms -10 us', now)
    datetime.timedelta(6, 82859, 990)
    >>>
    >>> makeTime('2012-10-11 16:36:44.41248000', now)
    datetime.datetime(2012, 10, 11, 16, 36, 44, 412480)
    """
    if isinstance(intime, (datetime.datetime, datetime.timedelta)):
        return intime

    if now is None:
        now=datetime.datetime.now()

    if intime == 'min':
        return datetime.datetime.min
    if intime == 'max':
        return datetime.datetime.max

    try:
        intime = float(intime)
    except (TypeError, ValueError):
        pass

    if isinstance(intime, float):
        tv=float(intime)
        S=tv//1
        NS=(tv%1)*1e9
        intime=(int(S), int(NS))
    elif isinstance(intime, (int, long)):
        intime = (intime, 0)

    if isinstance(intime, tuple):
        S, NS = intime
        S=datetime.datetime.fromtimestamp(float(S))
        S+=datetime.timedelta(microseconds=NS/1000)
        return S

    if not isinstance(intime, (str,unicode)):
        raise ValueError('Input must be a tuple, number, or string.  Not %s'%type(intime))

    intime=intime.strip().lower()

    if intime=='now' or len(intime)==0:
        return now

    R = _fromAbsString(intime, time.mktime(now.timetuple()))
    if R is not None:
        return datetime.datetime.fromtimestamp(R[0]+R[1]*1e-9)

    # treat as relative
    M=intime.split()
    if len(M)%2==1:
        raise ValueError('unexpect ending of \'%s\''%intime)

    parts=defaultdict(float)
    for i in range(0,len(M),2):
        off, tag = float(M[i]), M[i+1]
        tag, sca = _units.get(tag, (tag,1))
        off*=sca
        if tag[-1]!='s':
            tag+='s'
        parts[tag]+=off
    return datetime.timedelta(**parts)

def makeTimeInterval(start, end, now=None):
    """Take two (possibly relative) times and return two absolute
    times.

    *start* and *end* may be in any format accepted by makeTime().

    >>> import datetime
    >>> now=datetime.datetime(2011, 3, 15, 12)
    >>> X=makeTimeInterval('12:01', '1 hour', now)
    >>> X[0]
    datetime.datetime(2011, 3, 15, 12, 1)
    >>> X[1]
    datetime.datetime(2011, 3, 15, 13, 1)
    >>>
    >>> X=makeTimeInterval('10:01', '-1 hour', now)
    >>> X[0]
    datetime.datetime(2011, 3, 15, 10, 1)
    >>> X[1]
    datetime.datetime(2011, 3, 15, 11, 0)
    >>>
    >>> X=makeTimeInterval('-2 hours', '-1 hour', now)
    >>> X[0]
    datetime.datetime(2011, 3, 15, 10, 0)
    >>> X[1]
    datetime.datetime(2011, 3, 15, 11, 0)
    >>>
    >>> X=makeTimeInterval('10:02', '11:02', now)
    >>> X[0]
    datetime.datetime(2011, 3, 15, 10, 2)
    >>> X[1]
    datetime.datetime(2011, 3, 15, 11, 2)
    >>>
    """
    if now is None:
        now=datetime.datetime.now()
    if end is None:
        end=now
    if start is None:
        start=now

    start, end = makeTime(start, now), makeTime(end, now)

    rstart=isinstance(start, datetime.timedelta)
    rend=isinstance(end, datetime.timedelta)

    if rstart and rend:
        # -2 hours : -1 hours
        # both referenced to current time
        start=now+start
        end=now+end
    elif rstart:
        # -2 hours : 12:01
        # start relative to end
        start=end+start
    elif rend:
        if end >= datetime.timedelta(0):
            # 12:01 : 15 min
            # end relative to start
            end=start+end
        else:
            # 12:01 : -5 hours
            # end relative to current time
            end=now+end

    if start>end:
        start, end = end, start

    return (start, end)

def _test():
    import doctest
    doctest.testmod()

if __name__=='__main__':
    _test()
