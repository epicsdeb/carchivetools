
import logging
_log = logging.getLogger(__name__)

import re, collections, time

from twisted.web.server import Site
from twisted.application.internet import TCPServer

class HandledError(Exception):
    pass

_wild = re.compile(r'(?:\\(.))|([*?])|([^*?\\]+)')

def wild2re(pat):
    """Translate a wildcard pattern into a regular expression
    
    >>> wild2re("hello")
    'hello'
    >>> wild2re("hello.")
    'hello\\\\.'
    >>> wild2re("he?lo.")
    'he.lo\\\\.'
    >>> wild2re(r"he?lo. wor\?d")
    'he.lo\\\\.\\\\ wor\\\\?d'
    >>> wild2re(r"hel*w\*rld")
    'hel.*w\\\\*rld'
    """
    out=""
    for esc, wc, txt in _wild.findall(pat):
        if wc=='?':
            out+='.'
        elif wc=='*':
            out+='.*'
        else:
            out+=re.escape(txt or esc)
    return out

class Cache(object):
    """Associative collection bounded in time and size.

    >>> C=Cache(maxcount=3, maxage=2)
    >>> len(C._values)
    0
    >>> C.set('A', 1, now=0)
    >>> len(C._values)
    1
    >>> C.get('A', now=0)
    1
    >>> C.set('B', 2, now=2)
    >>> C.get('A', now=2)
    1
    >>> C.get('A', 42, now=3)
    42
    >>> list(C._values)
    ['B']
    >>>
    >>> C.clear()
    >>> C.set('A', 2, now=0)
    >>> C.set('B', 3, now=0)
    >>> C.set('C', 4, now=0)
    >>> C.set('D', 5, now=0)
    >>> list(C._values)
    ['B', 'C', 'D']
    >>>
    >>> C.clear()
    >>> C.set('A', 1, now=0)
    >>> C.set('B', 3, now=0)
    >>> C.set('C', 4, now=0)
    >>> C._times['A']
    0
    >>> C.set('A', 42, now=3)
    >>> C._times['A']
    3
    >>> list(C._values)
    ['B', 'C', 'A']
    >>> C.set('D', 5, now=0)
    >>> list(C._values)
    ['C', 'A', 'D']
    >>>
    >>> C.get('A', now=4)
    42
    >>> C.set('A', 40, now=2)
    >>> C.get('A', now=4)
    42
    >>> C.set('A', 40, now=3)
    >>> C.get('A', now=4)
    42
    >>> C.set('A', 40, now=4)
    >>> C.get('A', now=4)
    40
    """
    def __init__(self, maxcount=100, maxage=30, clock=time.time):
        self.clock = clock
        self.maxcount, self.maxage = maxcount, maxage
        self._values = collections.OrderedDict()
        self._times = {}

    def clear(self):
        self._values.clear()
        self._times.clear()

    def get(self, key, defv=None, now=None):
        try:
            V, T = self._values[key], self._times[key]
        except KeyError:
            return defv

        if now is None:
            now = self.clock()
        if now-T>self.maxage:
            # expired
            V = defv
            del self._values[key]
            del self._times[key]
        return V

    def pop(self, key, defv=None, now=None):
        try:
            V, T = self._values.pop(key), self._times.pop(key)
        except KeyError:
            return defv

        if now is None:
            now = self.clock()
        if now-T>self.maxage:
            # expired
            V = defv
        return V

    def set(self, key, value, now=None):
        if now is None:
            now = self.clock()

        try:
            # Prevent older from overwriting
            if now<=self._times[key]:
                return
        except KeyError:
            pass

        self._values.pop(key, None)
        self._values[key] = value
        self._times[key] = now

        while len(self._values)>self.maxcount:
            # too large
            K, _ = self._values.popitem(last=False)
            del self._times[K]

import weakref
class LimitedSite(Site):
    """An HTTP Site which limits the maximum number of client connections.
    
    Additional connections will not be accepted
    """
    lport = None
    maxConnections = 10
    def __init__(self, *args, **kws):
        Site.__init__(self, *args, **kws)
        
        self._connections = weakref.WeakKeyDictionary()
        self._active = True

    def buildProtocol(self, addr):
        if self._active and len(self._connections)>self.maxConnections:
            self.lport.stopReading()
            _log.info('Throttling with %s/%s connections',len(self._connections), self.maxConnections )
            self._active = False
        proto = Site.buildProtocol(self, addr)
        if proto:
            self._connections[proto] = weakref.ref(proto, self._dec)
        _log.info('build %s %s', addr, proto)
        return proto

    def _dec(self, X):
        _log.info('clean %s', X)
        if not self._active and len(self._connections)<=self.maxConnections:
            self.lport.startReading()
            _log.info('Un-Throttling with %s/%s connections',len(self._connections), self.maxConnections )
            self._active = True

class LimitedTCPServer(TCPServer):
    """TCPServer service which adds the ListeningPort
    to the protocol factory
    """
    def _getPort(self):
        fact = self.args[1]
        fact.lport = port = TCPServer._getPort(self)
        return port

if __name__=='__main__':
    import doctest
    doctest.testmod()
