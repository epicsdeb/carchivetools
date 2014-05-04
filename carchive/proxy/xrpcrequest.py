# -*- coding: utf-8 -*-

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
