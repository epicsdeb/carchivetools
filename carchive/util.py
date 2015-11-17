"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""
from __future__ import absolute_import

import logging
_log = logging.getLogger(__name__)

import re, collections, time
from cStringIO import StringIO

from twisted.web.client import Agent
from twisted.web.server import Site
from twisted.application.internet import TCPServer
from twisted.internet import defer, protocol, error
from twisted.python import failure
#defer.Deferred.debug=1

from twisted.web.client import ResponseDone, ResponseFailed

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

class BufferingLineProtocol(protocol.Protocol):
    """A line based protocol which buffers lines and delivers them in bulk.

    Processing will be started when the buffer is full, or when the connection
    is closed.

    C{processLines} may return a Deferred for longer processing.
    The rx buffer will continue to fill while processing is in progress.
    If the buffer fills before processing completes, then
    transport C{Producer} will be stopped (pauseProducing()) until processing
    completes, then resumed.
    
    @ivar defer: A Deferred which fires after the protocol is closed, and processing completes.
    @type defer: C{Deferred}
    
    @ivar rx_buf_size: Number of bytes to buffer before processing
    @type rx_buf_size: C{int}
    """
    rx_buf_size = 2**20

    def __init__(self):
        # This Deferred will fire when the request ends with the result
        # of the final call to processLines
        self.defer = defer.Deferred(canceller=self._abort)
        # The internal Deferred which tracks our processing
        self._defer= defer.succeed(None)
        # Our last processing callback
        self._last = None
        self.active = True

    def _abort(self, _ignore):
        self.transport.stopProducing()

    def processLines(self, lines, prev=None):
        """Called with a list of strings.
        
        @param prev: holds the value returned from a previous call to processLines,
        or None for the first invokation.
        
        @return: May return a Deferred() which fires when processing is complete
        @rtype: C{Deferred} or any value
        """
        raise NotImplementedError()

    def connectionMade(self):
        self.rxbuf = StringIO()
        # trick cStringIO to allocate the full buffer size
        # to allow append w/o re-alloc
        self.rxbuf.seek(self.rx_buf_size+1024)
        self.rxbuf.write('x')
        self.rxbuf.truncate(0)

        self._nbytes, self._tstart = 0, time.time()
        self._tend = None

    def dataReceived(self, data):
        self._nbytes += len(data)
        self.rxbuf.write(data)
        if self.rxbuf.tell()<self.rx_buf_size:
            return # below threshold

        elif not self._defer.called or self._defer.paused:
            # buffer full and processing in progress
            # stop reading more until processing completes
            if self.active:
                self.transport.pauseProducing()
            self.active = False
            return

        # split into complete lines
        L = self.rxbuf.getvalue().split('\n')
        if len(L)==1:
            return # no newline found

        self.rxbuf.truncate(0)
        # any bytes after the last newline are a partial line
        self.rxbuf.write(L[-1])
        assert self._defer.called
        self._defer.addCallback(self._invoke, lines=L[:-1])

    @defer.inlineCallbacks
    def _invoke(self, V, lines):
        try:
            self._last = defer.maybeDeferred(self.processLines, lines, prev=V)
            V = yield self._last
            # processing complete
            if not self.active:
                # resume reading
                self.transport.resumeProducing()
                self.active = True
        except Exception:
            _log.exception("Exception in processLines")
            self.transport.stopProducing()
            raise
        defer.returnValue(V)

    def connectionLost(self, reason):
        if not isinstance(reason, failure.Failure):
            reason = failure.Failure(reason)

        if reason.check(ResponseFailed):
            if len(reason.value.reasons)>=1:
                subR = reason.value.reasons[0]
                if not isinstance(subR, failure.Failure):
                    subR = failure.Failure(subR)

                if subR.check(error.ConnectionDone):
                    # connection closed by user request (aka. count limit reached)
                    reason = subR
                    self.rxbuf.truncate(0)


        if reason.check(error.ConnectionDone, ResponseDone):
            self._tend = time.time()
            # normal completion
            if self.rxbuf.tell()>0:
                # process remaining
                lines = self.rxbuf.getvalue().split('\n')
                @self._defer.addCallback
                def _flush(V):
                    if len(lines[-1])>0:
                        # last line is incomplete
                        raise RuntimeError('connection closed with partial line')

                    return self._invoke(V, lines=lines[:-1])

        else:
            # abnormal connection termination
            # inject the connectionn failure in place of success
            # processing failure takes precedence
            _log.debug("BLP connectionLost %s", reason)
            @self._defer.addCallback
            def _oops(V):
                return reason

        self._defer.chainDeferred(self.defer)

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
        _log.debug('build %s %s', addr, proto)
        return proto

    def _dec(self, X):
        _log.debug('clean %s', X)
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

class LimitedAgent(Agent):
    """Coarse rate limiting for Agent requests.

    Limits the number of concurrent requests to maxRequests regardless
    of destination (we usually have only one).
    """
    def __init__(self, *args, **kws):
        M = self.maxRequests = kws.pop('maxRequests', 100)
        super(LimitedAgent,self).__init__(*args, **kws)
        self.sem = defer.DeferredSemaphore(M)

    def acquire(self):
        return self.sem.acquire()
    def release(self):
        return self.sem.release()

if __name__=='__main__':
    import doctest
    doctest.testmod()
