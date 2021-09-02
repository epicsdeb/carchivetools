# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

import logging
_log = logging.getLogger(__name__)

import time, datetime

try:
    from xmlrpc.client import dumps, Fault, escape
except ImportError:
    from xmlrpclib import dumps, Fault, escape

import numpy

from twisted.internet import defer

from ..date import makeTime
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
        _log.error("%s: %s", msg, self.request)
        self.request.write(dumps(Fault(code, msg),
                                       methodresponse=True))
        self.request.finish()
        self.defer = defer.succeed(None)

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

        self.defer = S = applinfo.search(pattern=self.args[1] or '^.*$')

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
        for name in R.keys():
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

# Some default meta-data
_static_meta = "<struct>\n"+\
"<member>\n<name>units</name>\n<value><string></string></value>\n</member>\n"+\
"<member>\n<name>warn_high</name>\n<value><double>0.0</double></value>\n</member>\n"+\
"<member>\n<name>alarm_high</name>\n<value><double>0.0</double></value>\n</member>\n"+\
"<member>\n<name>disp_high</name>\n<value><double>0.0</double></value>\n</member>\n"+\
"<member>\n<name>warn_low</name>\n<value><double>0.0</double></value>\n</member>\n"+\
"<member>\n<name>type</name>\n<value><int>1</int></value>\n</member>\n"+\
"<member>\n<name>alarm_low</name>\n<value><double>0.0</double></value>\n</member>\n"+\
"<member>\n<name>prec</name>\n<value><int>0</int></value>\n</member>\n"+\
"<member>\n<name>disp_low</name>\n<value><double>0.0</double></value>\n</member>\n"+\
"</struct>"


# Parts of the response, in order

# Once.  Opens array.
_values_start = "<?xml version='1.0'?>\n<methodResponse>\n<params>\n<param>\n<value><array><data>\n"

# For each PV.  Opens Struct, Opens array.
# macros: name, type, count
_values_head = "<value><struct>\n"+\
"<member>\n<name>name</name>\n<value><string>%(name)s</string></value>\n</member>\n"+\
"<member>\n<name>type</name>\n<value><int>%(type)s</int></value>\n</member>\n"+\
"<member>\n<name>meta</name>\n<value>"+_static_meta+"</value>\n</member>\n"+\
"<member>\n<name>count</name>\n<value><int>%(count)s</int></value>\n</member>\n"+\
"<member>\n<name>values</name>\n<value><array><data>\n"

# For each Pv.  For each sample.  Opens Struct, Opens array.
# macros: stat, sevr, secs, nano
_sample_head = "<value><struct>\n"+\
"<member>\n<name>stat</name>\n<value><int>%(stat)s</int></value>\n</member>\n"+\
"<member>\n<name>sevr</name>\n<value><int>%(sevr)s</int></value>\n</member>\n"+\
"<member>\n<name>secs</name>\n<value><int>%(secs)s</int></value>\n</member>\n"+\
"<member>\n<name>nano</name>\n<value><int>%(nano)s</int></value>\n</member>\n"

_sample_minmax = "<member>\n<name>min</name>\n<value><double>%(min)s</double></value>\n</member>\n"+\
"<member>\n<name>max</name>\n<value><double>%(max)s</double></value>\n</member>\n"

_sample_start = "<member>\n<name>value</name>\n<value><array><data>\n"

# use _encoder to emit a series of <value>...</value>

# For each Pv.  For each sample.  Closes array.  Closes Struct,
_sample_foot = "</data></array></value>\n</member>\n</struct>\n</value>\n"

# For each PV.  Closes array. Closes Struct.
_values_foot = "</data></array></value>\n</member>\n"+\
"</struct>\n</value>\n"

# Once.  Closes array.
_values_end = "</data></array></value>\n</param>\n</params></methodResponse>\n"

class ValuesRequest(XMLRPCRequest):
    # key, names, start_sec, start_nano, end_sec, end_nano, count, how
    argumentTypes = (int, list, int, int, int, int, int, int)
    def __init__(self, httprequest, args, applinfo=None):
        super(ValuesRequest, self).__init__(httprequest, args)
        self.applinfo = applinfo

        self._names = self.args[1]
        self._start = makeTime((self.args[2],self.args[3]))
        self._end = makeTime((self.args[4],self.args[5]))
        self._count_limit = self.args[6]
        self._how = self.args[7]

        if self._how not in [0,3]:
            self.returnError(406, "how=%s is not supported"%self._how)
            return
        elif self._count_limit<=0:
            self.returnError(407, "Invalid count=%s"%self._count_limit)
            return
        elif (self._end-self._start).total_seconds()<=0.0:
            # wierd query which Databrowser actually makes...
            _log.warn("Start time is after end time.  Returns zero samples")
            self.request.write(_values_start)
            for name in self._names:
                self.request.write(_values_head%{'name':name,
                                                 'type':2,
                                                 'count':1}
                                   +_values_foot)
            self.request.write(_values_end)
            self.request.finish()
            self.defer = defer.succeed(None)
            return

        self._cur_pv, self._first_val = None, True
        self._count = 0

        #TODO: throttle reply
        self.request.write(_values_start)

        self.defer = self.getPVs()

    @defer.inlineCallbacks
    def getPVs(self):
        for name in self._names:
            self._cur_pv = name
            self._first_val = True

            if self._how==3:
                # Request for plot binning
                C = yield self.applinfo.fetchplot(name, T0=self._start, Tend=self._end,
                                                  count=self._count_limit, callback=self.processRaw)

                if self._first_val and C==0:
                    # So the plot binning didn't return anything, which is a bug.
                    # We try to get the last raw data point so we can at least
                    # give the client something...
                    _log.warn('plotbin returned zero samples: %s', self._cur_pv)
                    C = yield self.applinfo.fetchraw(name,
                                                     T0=self._end,
                                                     Tend=self._end+datetime.timedelta(seconds=1),
                                                     count=1, callback=self.processRaw)

                if self._first_val and C==0:
                    # oh well, we tried.  Ensure that an empty array is returned
                    _log.warn('raw fallback returned zero samples: %s', self._cur_pv)
                    self.processRaw(numpy.zeros((0,0)), [])

            else:
                C = yield self.applinfo.fetchraw(name, T0=self._start, Tend=self._end,
                                                 count=self._count_limit, callback=self.processRaw)
                if C==0:
                    # oh well, we tried.  Ensure that an empty array is returned
                    _log.warn('raw returned zero samples: %s', self._cur_pv)
                    self.processRaw(numpy.zeros((0,0)), [])

            assert not self._first_val, "values header never sent"
            self.request.write(_values_foot)

        self.request.write(_values_end)
        self.request.finish()

    def processRaw(self, V, M):
        if self._first_val:
            # first callback for this PV, emit header
            self.request.write(_values_head%{'name':self._cur_pv,
                                             'type':_d2x[V.dtype],
                                             'count':V.shape[1]})
        self._first_val = False

        E = _encoder[_d2x[V.dtype]]

        for i in range(len(M)):
            SM = M[i]
            self.request.write(_sample_head%{'stat':SM['status'],
                                             'sevr':SM['severity'],
                                             'secs':SM['sec'],
                                             'nano':SM['ns']}
                               +_sample_start)

            self.request.write(''.join(map(E, V[i,:])))

            self.request.write(_sample_foot)

        self._count += len(M)

    _pv_template = [
        ('meanSample_%d(%s)', 0),
        ('minSample_%d(%s)', 1),
        ('maxSample_%d(%s)', 2),
        ('firstSample_%d(%s)', 3),
        ('lastSample_%d(%s)', 4),
    ]
