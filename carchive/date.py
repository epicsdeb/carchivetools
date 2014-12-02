# -*- coding: utf-8 -*-
"""
Date string format

Parsing of absolute and relative dates and times
into datetime.datetime or datetime.timedelta instances

Supported syntax:

  Absolute:
    "day/month[/year] hour:min[:sec[.fraction]]"
    "hour:min[:sec[.fraction]]"
    "now"

  Relative:
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

import datetime, time, re, sys
from collections import defaultdict

__all__ = ["makeTime", "makeTimeInterval", 'timeTuple']

# python provides no concrete implementations of tzinfo
# and even if the zone database is available (pytz module)
# there is no easy way to find the system timezone name
# in a format which pytz understands.
# I give up.  doctests will only work in US/Eastern

# match absolute date plus time
# DAY/MON/YEAR HOUR:MIN:SEC.FRAC
_abs=re.compile(r"""
  (?:
      ([0-9]{1,2}) / ([0-9]{1,2}) # D/M
        (?: / ([0-9]{1,4}) )? # Y
  \s+
  )? ([0-2]?[0-9]) : ([0-6]?[0-9]) # H:m
        (?: : ([0-6]?[0-9]) # S
            (?: . ([0-9]+) )? # F
        )?
""",re.X)

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

def timeTuple(dt):
    """Convert datetime object to (sec, nsec)

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
    """Convert a datetime object to a ISO 8601 UTC string representation

    eg. 2014-04-10T16:27:37.767454Z

    >>> import datetime
    >>> now=datetime.datetime(2011, 3, 15, 13)
    >>> isoString(now)
    '2011-03-15T17:00:00Z'
    """
    S, NS = timeTuple(dt)
    udt = datetime.datetime.utcfromtimestamp(S).replace(microsecond=NS/1000)
    return udt.isoformat('T')+'Z'

def makeTime(intime, now=None):
    """Turn *intime* into datetime or timedelta

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
    datetime.datetime(2011, 3, 15, 12, 1, 14, 123456)
    >>> makeTime( '14/3 12:01', now)
    datetime.datetime(2011, 3, 14, 12, 1)
    >>> makeTime( '14/3 12:01:14', now)
    datetime.datetime(2011, 3, 14, 12, 1, 14)
    >>> makeTime( '14/3/2012 12:01:14.123456', now)
    datetime.datetime(2012, 3, 14, 12, 1, 14, 123456)
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

    S = intime.split('.')
    if len(S)<=2:
        try:
            US = 0 if len(S)==1 else float('0.'+S[1])*1e6
            T = time.strptime(S[0], '%Y-%m-%d %H:%M:%S')
            return datetime.datetime(*(T[0:6]+(int(US),)))
        except ValueError:
            del S

    M=_abs.match(intime)
    if M is not None:
        # take missing pieces from now
        G=M.groups()[0:6]
        G=G[2::-1]+G[3:] # swap day and year
        G=zip(G, now.timetuple()[0:6])
        G=[int(A) if A is not None else B for A,B in G]

        US=int(float('0.'+(M.group(7) or '0'))*1e6)

        Y, M, D, H, m, S = G
        return datetime.datetime(year=Y, month=M, day=D, hour=H,
                                 minute=m, second=S, microsecond=US,
                                 tzinfo=tzinfo)

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
    print 'All tests have run'
