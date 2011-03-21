# -*- coding: utf-8 -*-

# supported RPC call version
PVER=0

import logging, xmlrpclib
from collections import defaultdict
from fnmatch import fnmatch
_log = logging.getLogger("carchive.archiver")

from _conf import _conf
from date import makeTimeInterval, timeTuple
from data import DataHolder
from query import ArchiveQuery

__all__ = ['Archiver']

class Archiver(object):
    """Archiver("shortname" [, conf=ConfigParser])
    Archiver("http://..." [, conf=ConfigParser])
    Archiver(tx=xmlrpc.ServerProxy() [, conf=ConfigParser])
    
    Handle for accessing a data server.
    
    *name* can be a section name from *conf*, a full url,
    or a host name (uses)
    
    Methods throw ValueError, or xmlrpclib.Fault on errors
    """
    
    def __init__(self, name=None, tx=None, conf=_conf, debug=False):
        if tx is None:
            url=None
            assert name is not None, "Must give name or ServerProxy"
            
            if conf.has_section(name):
                url=conf.get(name,'url')
                url=url%{'host':name} # expand if needed
            elif name.startswith('http'):
                url=name
            else:
                url=conf.get('_unspecified_','url')
                url=url%{'host':name} # expand

            assert url is not None, 'Could not determine server URL'
            tx=xmlrpclib.ServerProxy(url, verbose=debug)
        self.__tx=tx
        self.__conf=conf
        
        self.reset()

    def reset(self):
        """Flush cached information from the server.
        Next operation will re-request values from the server.
        """
        self.__archs={} # {str:int}
        self.__rarchs={} # {int:str}
        self.__sevr={} # {int:(str,bool,bool)}
        self.__stat={} # {int:str}
        self.__desc='<not connected>'
        self.__how={} # {str:int}

    def archs(self, pattern='*'):
        """List available archives.
        
        *pattern* can use shell style wildcards (eg. "vac/*")
        as well as exact strings.
        """
        self._cache()
        return filter(lambda a:fnmatch(a, pattern), self.__archs.iterkeys())

    def search(self, pattern, archs=None):
        """Search for channel names
        
        Returns matching channels in a dictionary
        with channel name as the key, and a list of
        time ranges [(start, end, archive)] as the value
        
        @pattern - used to match names
        @archs - list of archive keys to search
        """
        self._cache()
        if archs is None:
            archs=self.__archs.values()

        res=defaultdict(list)

        for a in archs:
            if not isinstance(a,int):
                a=self.__archs[a]

            R=self.__tx.archiver.names(a, pattern)

            for r in R:
                S=r['start_sec'] + r['start_nano']/1e9
                E=r['end_sec']   + r['end_nano']  /1e9
                res[r['name']].append((S,E, self.__rarchs.get(a,'<unknown>')))

        for data in res.itervalues():
            # sort chunks in time order
            data.sort(key=lambda x:x[0])

        return res

    def get(self, names, start, end, count=10, how='raw', archs=None):
        """Retrieve data.
        
        *names* is a list of name strings [str]
        *start* and *end* can be strings, numbers (seconds),
            or a tuple (sec,nsec).
        *count* is the number of data points to request per channel.
        *how* is the retrieve methods.  Can be a string or integer key.
        *args* a list of archive names or keys
        
        Returns a dictionary mapping channel name to a list of
        DataHolder instances.  Each list entry represents a time
        range.  Entries are sorted in time order, but ranges may
        overlap or have gaps.
        
        holder.value will be a list of values.  Each value may itself
        by a list (for waveforms)
        """
        assert not isinstance(names, str), 'Expecting a list of strings'
        self._cache()

        start, end = makeTimeInterval(start, end)
        start, end = timeTuple(start), timeTuple(end)
        how = self.__how.get(how,how)

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
                if len(ent['values'])==0:
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

    @property
    def sevr(self):
        """{int:(str,bool,bool)}
        Infomation of severity codes
        """
        self._cache()
        return self.__sevr

    @property
    def stat(self):
        """{int:str}
        Information on status codes
        """
        self._cache()
        return self.__stat

    @property
    def how(self):
        """[str]
        List of supported retrieval methods
        """
        self._cache()
        return self.__how.keys()

    @property
    def desc(self):
        """str
        Archiver description
        """
        self._cache()
        return self.__desc

    def Q(self):
        """Return an new ArchiveQuery instance
        
        See *carchive.query* module for usage
        """
        return ArchiveQuery(self)

    def __unicode__(self):
        self._cache()
        return u'Server: %s\nHow: %s'%(self.desc, ', '.join(self.how))

    def __str__(self):
        return str(unicode(self))

    def _cache(self):
        if len(self.__how)>0:
            return

        I=self.__tx.archiver.info()

        if I['ver']!=PVER:
            raise RuntimeError('Dataserver protocol version %d not supported (%d)'%\
                               (I['ver'],PVER))

        self.__desc=I['desc']

        how=dict([(H,i) for i,H in enumerate(I['how'])])
        self.__stat=dict([(H,i) for i,H in enumerate(I['stat'])])

        for s in I['sevr']:
            self.__sevr[s['num']]= (s['sevr'], s['has_value'], s['txt_stat'])

        A=self.__tx.archiver.archives()
        for a in A:
            self.__archs[a['name']]=a['key']
            self.__rarchs[a['key']]=a['name']

        self.__how=how
