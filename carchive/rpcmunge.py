"""
Need to make the standard HTTPClient more forgiving
of non-standard HTTP servers.

In particular handle headers with '\n' instead of '\n\r'
which is perpetrated by xmlrpc-c
"""

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
    protocol = NiceQueryProtocol

class NiceProxy(Proxy):
    queryFactory = NiceQueryFactory
