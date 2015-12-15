# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""
from __future__ import absolute_import

try:
  from ConfigParser import (SafeConfigParser as ConfigParser, NoSectionError, NoOptionError)
except ImportError:
  from configparser import ConfigParser, NoSectionError, NoOptionError

import six

class ConfigDict(object):
    def __init__(self, P, S):
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
        return dict(six.iteritems(self))

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
    cf=ConfigParser(defaults=dflt)
    cf.read([
        '/etc/carchive.conf',
        os.path.expanduser('~/.carchiverc'),
        'carchive.conf'
    ])
    return ConfigDict(cf, N)
