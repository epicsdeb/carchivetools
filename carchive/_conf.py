# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

try:
    from configparser import SafeConfigParser, NoOptionError, NoSectionError
except ImportError:
    from ConfigParser import SafeConfigParser, NoOptionError, NoSectionError

class ConfigDict(object):
    """dict-like wrapper around a ConfigParser

    >>> P=SafeConfigParser()
    >>> P.set('DEFAULT', 'foo', 'bar')
    >>> P.set('DEFAULT', 'baz', '5')
    >>> D=ConfigDict(P, 'DEFAULT')
    >>> D['foo']
    'bar'
    >>> D['baz']
    '5'
    >>> D.get('baz')
    '5'
    >>> D.getint('baz')
    5
    >>> D.get('unknown')
    >>> D.getint('unknown', 42)
    42
    >>> D=ConfigDict({'A':'B', 'C':'4'})
    >>> D['A']
    'B'
    >>> D.getint('C')
    4
    >>>
    """
    def __init__(self, P, S='DEFAULT'):
        if isinstance(P, dict):
            D = P
            P = SafeConfigParser()
            if S!='DEFAULT':
                P.add_section(S)
            for K,V in D.items():
                P.set(S, K, V)
        self._P, self._S = P, S

    def __iter__(self):
        return iter(self._P.options(self._S))

    def iteritems(self):
        for K in self._P.options(self._S):
            yield (K, self[K])

    def __contains__(self, k):
        return self._P.has_option(self._S, k)

    def __getitem__(self, k):
        try:
            return self._P.get(self._S, k)
        except (NoOptionError, NoSectionError):
            raise KeyError("Section %s has no key %s"%(self._S, k))

    def __setitem__(self, k, v):
        self._P.set(self._S, k, v)

    def set(self, k, v):
        self._P.set(self._S, k, v)

    def get(self, k, d=None):
        try:
            return self._P.get(self._S, k)
        except (NoOptionError, NoSectionError):
            return d

    def getint(self, k, d=None):
        try:
            return self._P.getint(self._S, k)
        except (NoOptionError, NoSectionError):
            return d

    def getfloat(self, k, d=None):
        try:
            return self._P.getfloat(self._S, k)
        except (NoOptionError, NoSectionError):
            return d

    def getboolean(self, k, d=None):
        try:
            return self._P.getboolean(self._S, k)
        except (NoOptionError, NoSectionError):
            return d
        
    def write(self, fd):
        self._P.write(fd)

    def todict(self):
        return dict(self.iteritems())

    def __str__(self):
        return str(self.todict())

    __repr__ = __str__

def loadConfig(N):
    import os.path
    dflt={'url':'http://%(host)s/cgi-bin/ArchiveDataServer.cgi',
          'urltype':'classic',
          'host':'%%(host)s',
          'defaultarchs':'*',
          'defaultcount':'0',
          'maxquery':'30',
        }
    cf=SafeConfigParser(defaults=dflt)
    cf.read([
        '/etc/carchive.conf',
        os.path.expanduser('~/.carchiverc'),
        'carchive.conf'
    ])
    return ConfigDict(cf, N)
