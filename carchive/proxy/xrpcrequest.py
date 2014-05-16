# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

import time

from xmlrpclib import dumps, Fault, escape

from ..date import isoString,makeTime
from ..backend.appl import _dtypes

class XMLRPCRequest(object):
    def __init__(self, httprequest, args):
        self.request = httprequest
        self.args = args
        self.defer = None

        if len(args)!=len(self.argumentTypes):
            httprequest.write(dumps(Fault(401, "Wrong number of args"),
                                    methodresponse=True))
            httprequest.finish()
            return

        for R,E in zip(args, self.argumentTypes):
            if not isinstance(R, E):
                httprequest.write(dumps(Fault(402, "Invalid argument type"),
                                        methodresponse=True))
                httprequest.finish()
                return

        self._complete = httprequest.notifyFinish()
        self._complete.addCallback(self.normalEnd)
        self._complete.addErrback(self.abortEnd)

    def returnError(self, code, msg):
        """Return an XMLRPC error.
        """
        self.request.write(dumps(Fault(code, msg),
                                       methodresponse=True))
        self.request.finish()

    def normalEnd(self, R):
        return R

    def abortEnd(self, R):
        if self.defer:
            self.defer.cancel()
        return R


class NamesRequest(XMLRPCRequest):
    # key, pattern
    argumentTypes = (int, str)
    def __init__(self, httprequest, args, applinfo=None):
        super(NamesRequest, self).__init__(httprequest, args)
        self.applinfo = applinfo
        pattern = self.args[1]

        # ArchiveDataServer looks for partial matches
        # Archive Appliance matches the entire line (implicit ^...$)
        if not pattern:
            pattern='.*'
        else:
            if not pattern.startswith('^'):
                pattern='.*'+pattern
            if not pattern.endswith('$'):
                pattern=pattern+'.*'

        self.defer = S = applinfo.search(pattern)

        S.addCallback(self.results)
        S.addErrback(self.error)

    def results(self, R):
        """Have results
        """
        now = time.time()
        static = {
            'start_sec':0, 'start_nano':0,
            'end_sec':now, 'end_nano':0
        }
        rep = []
        for name in R:
            D = {'name':name}
            D.update(static)
            rep.append(D)
        self.request.write(dumps((rep,), methodresponse=True))
        self.request.finish()

    def error(self, R):
        """Appliance request failed.  Notify client.
        """
        self.returnError(405, str(R))
        return R

# map PayloadType (via numpy value dtype) to XMLRPC type code
_d2x = {
    _dtypes[0]:0, # string
    _dtypes[1]:2, # int
    _dtypes[2]:3, # double
    _dtypes[3]:2, #TODO: should be enum (1)
    _dtypes[4]:2,
    _dtypes[5]:2,
    _dtypes[6]:3,
    _dtypes[7]:0,
    _dtypes[8]:2,
    _dtypes[9]:3,
    _dtypes[10]:2, #TODO: should be enum (1)
    _dtypes[11]:2,
    _dtypes[12]:2,
    _dtypes[13]:3,
    _dtypes[14]:2,
}

_encoder = {
    0:lambda v:"<value><string>%s</string></value>"%escape(v),
    2:lambda v:"<value><int>%s</int></value>"%str(int(v)),
    3:lambda v:"<value><double>%s</double></value>"%repr(v),
}

class ValuesRequest(XMLRPCRequest):
    # key, names, start_sec, start_nano, end_sec, end_nano, count, how
    argumentTypes = (int, list, int, int, int, int, int, int)
    def __init__(self, httprequest, args, applinfo=None):
        super(ValuesRequest, self).__init__(httprequest, args)
        self.applinfo = applinfo

        self._names = self.args[1]
        self._start = isoString(makeTime((self.args[2],self.args[3])))
        self._end = isoString(makeTime((self.args[4],self.args[5])))
        self._count_limit = self.args[6]
        self._how = self.args[7]

        if self._how!=0:
            self.returnError(406, "how=%s is not supported"%self._how)
            return
        elif self._count_limit<0:
            self.returnError(407, "Invalid count=%s"%self._count_limit)
            return
        elif self.args[2]>self.args[4]:
            self.returnError(408, "Start time is after end time")
            return

        #TODO: write response header
        self.request.write("<?xml version='1.0'?>\n<methodResponse>\n<params>\n<param>\n<value><array><data>\n")

        self.defer = self.getPV(None)

    def getPV(self, V):
        pv = self._cur_pv = self._names.pop(0)

        self._count = 0

        D = self.applinfo.fetch(pv, start=self._start, end=self._end,
                                count=self._count_limit, cb=self.process)

        if len(self._names)>0:
            # More PVs
            D.addCallback(self.getPV)
        else:
            D.addCallback(self.footer)

        return D

    def process(self, V, M):
        pass

    def footer(self):
        self.request.write("</data></array></value>\n</param>\n</params>\n")
        self.request.finish()
