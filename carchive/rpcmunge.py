"""
Need to make the standard HTTPClient more forgiving
of non-standard HTTP servers.

In particular handle headers with '\n' instead of '\n\r'
which is perpetrated by xmlrpc-c

Also implement throttling of the number of outstanding queries
"""

import logging
_log = logging.getLogger("carchive.rpcmunge")

from twisted.internet import defer

#from twisted.web.http import HTTPClient
from twisted.web.xmlrpc import QueryProtocol, _QueryFactory, Proxy

class NiceQueryProtocol(QueryProtocol):
    def lineReceived(self, line):
       # Pass through end of header
       if not line:
           QueryProtocol.lineReceived(self, line)
           return

       # seperate with any single valid EOL combination.
       # '\n\n' becomes ['','']
       lines = line.splitlines()

       # Pass one at a time until we pass the header
       while self.line_mode and len(lines):
           line = lines.pop(0)
           QueryProtocol.lineReceived(self, line)

       # Any remaining lines are really part of the body
       # join with arbitrary EOL since XML parser should
       # be able to handle it.
       if len(lines):
           self.rawDataReceived('\n\r'.join(lines))
           

class NiceQueryFactory(_QueryFactory):
    noisy = False
    protocol = NiceQueryProtocol

class NiceProxy(Proxy):
    queryFactory = NiceQueryFactory

    def __init__(self, *args, **kws):
        self.__limit = kws.pop('limit', 10)
        self.__qlimit = kws.pop('qlimit', 10)
        Proxy.__init__(self, *args, **kws)
        self.__inprog = 0
        self.__waiting = []

    def callRemote(self, *args):
        lim = self.__limit
        if args[0]!='archiver.values':
            lim = self.__qlimit
        if self.__inprog<lim:
            _log.debug("Immedate request execution: %s", args)
            D = Proxy.callRemote(self, *args)
            D.addBoth(self.__complete)
            self.__inprog += 1
            return D

        _log.debug("Delay request until later: %s", args)
        D = defer.Deferred()
        self.__waiting.append((D,args))
        return D

    def __complete(self, R):
        self.__inprog -= 1
        if len(self.__waiting):
            D, args = self.__waiting.pop(0)
            _log.debug("Delayed request now executing: %s", args)
            D2 = Proxy.callRemote(self, *args)
            D2.addBoth(self.__complete)
            D2.chainDeferred(D)
            self.__inprog += 1
        else:
            _log.debug("No delayed requests pending. %d requesting in progress",
                       self.__inprog)
        return R
