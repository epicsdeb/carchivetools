# -*- coding: utf-8 -*-
"""
Created on Sat Mar 19 18:39:28 2011

@author: -
"""

# supported RPC call version
PVER=0

import logging, datetime, time, re
from collections import defaultdict
_log = logging.getLogger("carchive")

import xmlrpclib

try:
    import numpy
except ImportError:
    _log.debug('Numpy not available, falling back to lists')
    numpy=None

def __loadConfig():
    import os.path
    import ConfigParser
    cf=ConfigParser.SafeConfigParser()
    cf.read([
        '/etc/carchive.conf',
        os.path.expanduser('~/.carchiverc'),
        'carchive.conf'
    ])
    if not cf.has_section('generic'):
        cf.add_section('generic')
    return cf
_conf=__loadConfig()



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

def timeTuple(dt):
    """Convert datetime object to (sec, nsec)
    """
    S=time.mktime(dt.timetuple())
    NS=dt.microsecond*1000
    return S,NS

def makeTime(intime, now=None):
    """Turn anything into datetime or timedelta
    
    Note: Time may be specified to microsecond precision
    >>> import datetime
    >>> now=datetime.datetime(2011, 3, 15, 12)
    >>> makeTime(now,now)==now
    True
    >>> makeTime(65,now)
    datetime.datetime(1970, 1, 1, 0, 1, 5)
    >>>
    >>> makeTime(1300584688.9705319,now)
    datetime.datetime(2011, 3, 20, 1, 31, 28, 970531)
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
    """
    if isinstance(intime, (datetime.datetime, datetime.timedelta)):
        return intime

    if now is None:
        now=datetime.datetime.now()

    if isinstance(intime, (float, int, long)):
        tv=float(intime)
        S=tv//1
        NS=(tv%1)*1e9
        intime=(int(S), int(NS))

    if isinstance(intime, tuple):
        S, NS = intime
        S=datetime.datetime.utcfromtimestamp(S)
        S+=datetime.timedelta(microseconds=NS/1000)
        return S

    if not isinstance(intime, str):
        raise ValueError('Input must be a tuple, number, or string')

    intime=intime.strip().lower()

    if intime=='now' or len(intime)==0:
        return now

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
                                 minute=m, second=S, microsecond=US)

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

if numpy:
    _typecodes = {1: numpy.int32,
                  2: numpy.int32,
                  3: numpy.float64}

class DataHolder(object):
    __slots__=['name', 'type', 'how',
               'value', 'status', 'severity', 'timestamp',
               'enums', 'precision', 'units',
               'upper_disp_limit','lower_disp_limit',
               'upper_ctrl_limit','lower_ctrl_limit',
               'upper_alarm_limit','lower_alarm_limit',
               'upper_warning_limit','lower_warning_limit',
               '__weakref__']

class Archiver(object):
    """Token for accessing a data server
    """
    
    def __init__(self, name='generic', url=None, tx=None, conf=_conf):
        if url is None and conf.has_option(name,'url'):
            url=conf.get(name,'url')            
        if tx is None:
            assert url is None, 'Could not determine server URL'
            tx=xmlrpclib.ServerProxy(url)
        self.__tx=tx
        self.__conf=conf
        
        self.reset()

    def reset(self):
        """Clear all cached information from the server
        """
        self.__archs={} # {str:int}
        self.__sevr={} # {int:(str,bool,bool)}
        self.__stat={} # {int:str}
        self.__desc='<not connected>'
        self.__how={} # {str:int}     

    def archs(self):
        """List available archives
        """
        self._cache()
        return self.__archs.items()
    archs=property(archs)

    def search(self, pattern, archs=None):
        """Search for channel names
        
        Returns matching channels in a dictionary
        with channel name as the key, and a list of
        time ranges [(start, end)] as the value
        
        @pattern - used to match names
        @archs - list of archive keys to search
        """
        if archs is None:
            archs=self.__archs.values()

        res=defaultdict(list)

        for a in archs:
            if not isinstance(a,int):
                a=self.__archs[a]

            R=self.__tx.archiver.names(a, pattern)

            for N, SS, SN, ES, EN in R:
                S=SS + SN/1e9
                E=ES + EN/1e9
                res[N].append((S,E))

        return res

    def get(self, names, start, end, count=10, how=1, archs=None):
        """Retrieve data
        """
        assert not isinstance(names, str), 'Expecting a list of strings'

        start, end = makeTimeInterval(start, end)
        start, end = timeTuple(start), timeTuple(end)

        if archs is None:
            archs=self.__archs.values()

        res=defaultdict(list)

        for a in archs:
            # search each of the requested archives
            if not isinstance(a,int):
                a=self.__archs[a]

            R=self.__tx.archiver.values(a, names,
                                        start[0], start[1],
                                        end[0], end[1],
                                        count, how)

            for ent in R:
                # process results for each channel of each archive
                if ent['count']<=0:
                    _log.warning('Server returned no data for %s from %s',
                                 ent['name'], a)
                    continue
                D=DataHolder()
                D.how=how
                D.name=ent['name']

                # range meta-data
                M=ent['meta']
                MT=M['type']
                if MT==0:
                    D.enums=M['states']

                elif MT==1:
                    D.upper_disp_limit=M['disp_high']
                    D.lower_disp_limit=M['disp_low']
                    D.upper_alarm_limit=M['alarm_high']
                    D.lower_alarm_limit=M['alarm_low']
                    D.upper_warning_limit=M['warn_high']
                    D.lower_warning_limit=M['warn_low']
                    D.precision=M['prec']
                    D.units=M['units']

                else:
                    _log.warning('Server reported unknown meta-type %s',MT)

                # values
                D.type=vtype=ent['type']
                raw=ent['values']
                assert len(raw)==ent['count'], 'Data size inconsistent %d %d'%\
                    (len(raw),ent['count'])

                sts=D.status=[0]*len(raw)
                sevr=D.severity=[0]*len(raw)
                T=D.timestamp=[(0,0)]*len(raw)

                V=D.value=[None]*len(raw)

                for i, pnt in enumerate(raw):
                    V[i]=pnt['value']
                    sts[i]=pnt['stat']
                    sevr[i]=pnt['sevr']
                    T[i]=(pnt['secs'],pnt['nano'])

                res[D.name].append(D)

        for data in res.itervalues():
            # sort chunks in time order
            data.sort(key=lambda x:x.timestamp[0])

        return res

    def _cache(self):
        if len(self.__how)>0:
            return

        I=self.__tx.archiver.info()

        if I['ver']!=PVER:
            raise RuntimeError('Dataserver protocol version %d not supported (%d)'%\
                               (I['ver'],PVER))

        self.__desc=I['desc']

        how=dict([(H,I) for I,H in enumerate(I['how'])])
        self.__stat=dict([(H,I) for I,H in enumerate(I['stat'])])

        for s in I['sevr']:
            self.__sevr[s['num']]= (s['sevr'], s['has_value'], s['txt_stat'])

        A=self.__tx.archiver.archives()
        for a in A:
            self.__archs[a['name']]=a['key']

        self.__how=how

if __name__=='__main__':
    import doctest
    doctest.testmod()
