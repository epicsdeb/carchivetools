# -*- coding: utf-8 -*-

import time

from xmlrpclib import dumps, Fault

class XMLRPCRequest(object):
    def __init__(self, httprequest, args):
        self.request = httprequest
        self.args = args

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
        self._complete.addCallbacks(self.normalEnd, self.abortEnd)

    def normalEnd(self, R):
        return R

    def abortEnd(self, R):
        return R


class NamesRequest(XMLRPCRequest):
    # key, pattern
    argumentTypes = (int, str)
    def __init__(self, httprequest, args, applinfo=None):
        super(NamesRequest, self).__init__(httprequest, args)
        self.applinfo = applinfo

        self._remote = S = applinfo.search(self.args[1])

        S.addCallbacks(self.results, self.error)

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
        self.request.write(dumps(rep, methodresponse=True))
        self.request.finish()

    def error(self, R):
        """Appliance request failed.  Notify client.
        """
        self.request.write(dumps(Fault(405, str(R)),
                                       methodresponse=True))
        self.request.finish()

    def abortEnd(self, R):
        """Client closed connection early
        """
        if self._remote:
            self._remote.cancel()
        self._remote = None

class ValuesRequest(XMLRPCRequest):
    # key, names, start_sec, start_nano, end_sec, end_nano, count, how
    argumentTypes = (int, list, int, int, int, int, int, int)
    def __init__(self, httprequest, args, applinfo=None):
        super(ValuesRequest, self).__init__(httprequest, args)
