# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

from xmlrpclib import loads, dumps, Fault

from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.python.failure import Failure

import applinfo

from .xrpcrequest import NamesRequest, ValuesRequest

_info = {
    'ver':0,
    'desc':'Archiver to Applicance gateway',
    # list all the possible "how"s even though we won't support them
    'how':['raw', 'spreadsheeet', 'averaged', 'plot-binning', 'linear'],
    'stat':map(str, range(22)),
    'sevr':[
        {'num':0, 'sevr':'NO_ALARM','has_value':True, 'txt_stat':True},
        {'num':1, 'sevr':'MINOR',   'has_value':True, 'txt_stat':True},
        {'num':2, 'sevr':'MAJOR',   'has_value':True, 'txt_stat':True},
        {'num':3, 'sevr':'INVALID', 'has_value':True, 'txt_stat':True},
        {'num':3968, 'sevr':'Est_Repeat',      'has_value':True,  'txt_stat':False},
        {'num':3856, 'sevr':'Repeat',          'has_value':True,  'txt_stat':False},
        {'num':3904, 'sevr':'Disconnect',      'has_value':False, 'txt_stat':True},
        {'num':3872, 'sevr':'Archive_Off',     'has_value':False, 'txt_stat':True},
        {'num':3848, 'sevr':'Archive_Disable', 'has_value':False, 'txt_stat':True},
    ],
}
_info_rep = dumps((_info,), methodresponse=True)

_archives = [
    {'key':42, 'name':'All', 'path':'All Data'},
]
_archives_rep = dumps((_archives,), methodresponse=True)


def cleanupRequest(R, req):
    print 'cleanup',req,R
    if not req._disconnected:
        if not req.startedWriting:
            req.setResponseCode(500)
            req.write("")
        if not req.finished:
            req.unregisterProducer()
            req.finish()
    if isinstance(R, Failure):
        try:
            R.raiseException()
        except:
            _log.exception("Unhandled execption during request: %s", req)


class DataServer(Resource):
    isLeaf=True
    NamesRequest=NamesRequest
    ValuesRequest=ValuesRequest
    def render_GET(self, req):
        return "Nothing to see here.  Make an XMLRPC request"

    def render_POST(self, req):
        if req.content is None:
            req.setResponseCode(400)
            return 'Missing request body'
        # Twisted stores the request body in either StringIO or TempFile
        # so the following might result in file I/O, but won't
        # read from the request transport (circa Twisted 12.0)
        try:
            args, meth = loads(req.content.read())
            _log.info("Request: %s%s", meth,args)
        except Exception as e:
            _log.exception("Error decoding request: ")
            req.setResponseCode(400)
            return e.message

        req.setHeader('Content-Type', 'text/xml')

        try:
            if meth=='archiver.info':
                return _info_rep
            elif meth=='archiver.archives':
                return _archives_rep
            elif meth=='archiver.names':
                _log.debug("%s: archiver.names %s",
                           req.getClientIP(), args)
                D = self.NamesRequest(req, args, applinfo=self.applinfo)
            elif meth=='archiver.values':
                _log.debug("%s: archiver.values %s",
                           req.getClientIP(), args)
                D = self.ValuesRequest(req, args, applinfo=self.applinfo)
            else:
                _log.error("%s: Request for unknown method %s",
                           req.getClientIP(), meth)
                return dumps(Fault(400, "Unknown method"),
                             methodresponse=True)
        except:
            _log.exception("Failure starting response: %s", req)
            cleanupRequest(None, req)
        else:
            D.defer.addBoth(cleanupRequest, req)

        return NOT_DONE_YET

def buildResource(infourl=None):
    I = applinfo.ApplInfo(infourl)
    root= Resource()
    cgibin = Resource()
    root.putChild('cgi-bin', cgibin)
    C = DataServer()
    C.applinfo = I
    cgibin.putChild('ArchiveDataServer.cgi', C)
    return root

def main():
    from twisted.web.server import Site
    from twisted.internet import reactor
    root = buildResource()
    reactor.listenTCP(8888, Site(root), interface='127.0.0.1')
    reactor.run()

if __name__=='__main__':
    main()
