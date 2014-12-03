
import logging
_log = logging.getLogger(__name__)

import re

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
