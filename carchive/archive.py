# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

import threading

from twisted.python import failure
from twisted.internet import defer

try:
    from .backend import classic
except ImportError:
    if _log.isEnabledFor(logging.DEBUG):
        _log.exception("Failed to import classic backend")
    classic=None
try:
    from .backend import appl
except ImportError:
    if _log.isEnabledFor(logging.DEBUG):
        _log.exception("Failed to import appliance backend")
    appl=None

def getArchive(conf):
    """Returns a twisted.defer.Deferred which will
    complete with an object implementing IArchive.
    
    The argument 'conf' must be a dict()-like object.
    This can be constructed with archiver._conf.loadConfig()
    """
    if conf['urltype']=='classic' and classic:
        return classic.getArchive(conf)
    elif conf['urltype']=='appl' and appl:
        return appl.getArchive(conf)
    raise ValueError("Unsupported urltype: %s"%conf['urltype'])

class ReactorRunner(object):
    """Helper to manage running the twisted reactor in a worker thread
    """
    daemon = True
    def __init__(self, reactor=None):
        from twisted.internet import reactor as _reactor
        self.reactor = reactor or _reactor
        self._T = None

    def start(self):
        assert not self._T
        assert not self.reactor.running
        self._T = threading.Thread(name='twisted', target=self.reactor.run,
                                   kwargs={"installSignalHandlers":0})
        self._T.daemon = self.daemon
        self._T.start()

    def stop(self):
        assert self._T
        assert self.reactor.running
        self.reactor.callFromThread(self.reactor.stop)
        self._T.join()
        self._T = None

    def call(self, fn, *args, **kws):
        """Execute a function on the reactor thread
        and block until it completes.

        If the function initiates a deferred action, then
        blocking continues until this action completes.
        """
        E = threading.Event()
        result = [None]
        def wrapper():
            D = defer.maybeDeferred(fn, *args, **kws)
            @D.addBoth
            def done(R):
                result[0] = R
                E.set()
        self.reactor.callFromThread(wrapper)
        E.wait()
        if isinstance(result[0], failure.Failure):
            result[0].raiseException()
        else:
            return result[0]

    def callAll(self, fns, throw=True, **kws):
        """Execute a list of callables on the reactor thread.
        Block until completion conditions are met.
        By default completion is on the first failure or the last success.
        Additional arguments passed to twisted.internet.defer.DeferredList().
        
        If throw=True, then the first failure is raised as an exception.
        Otherwise a twisted.python.Failure is returned.
        
        Returns a list of the result of each callable, or a Failure
        if an exception was thrown.
        """
        kws.setdefault('fireOnOneErrback', True)
        kws.setdefault('consumeErrors', True)
        dothrow = kws.pop('throw', True)
        E = threading.Event()
        result = [None]
        def wrapper():
            Ds = [defer.maybeDeferred(fn, *a, **k) for fn,a,k in fns]
            S = defer.DeferredList(Ds, **kws)
            @S.addBoth
            def done(R):
                result[0] = R
                E.set()
        self.reactor.callFromThread(wrapper)
        E.wait()
        R = [None]*len(result[0])
        for i,(ok,res) in enumerate(result[0]):
            if dothrow and not ok:
                res.raiseException()
            R[i] = res
        return R
        
try:
    from zope.interface import Interface
except ImportError:
    pass
else:
    class IArchive(Interface):
        def archives(pattern):
            """Return a list of archive sections matching
            the given wildcard pattern.
            """

        def lookupArchive(key):
            "Lookup the string name of an archive from its ID number"

        def severity(i):
            "Fetch the name string for the given alarm severity"

        def status(stat):
            "Fetch the name string for the given alarm status"

        def search(pattern=None,
                   archs=None, breakDown=False):
            """Lookup PV names matching a pattern.

            The only required argument is 'pattern'.

            If 'archs' is provided, it must be a list of archive section
            names or ID numbers.  If not provided, all archive sections are
            searched (may be slow).

            This function returns a twisted.defer.Deferred which will
            complete with a result depending on the following.

            If breakDown is False (the default) then the result is
            {'pvname':(firstTime, lastTime)}

            If breakDown is True then the result is
            {'pvname':[(firstTime, lastTime, archKey)]}
            """

        def fetchraw(pv, callback,
                     cbArgs=(), cbKWs={},
                     T0=None, Tend=None,
                     count=None, chunkSize=None,
                     archs=None,
                     enumAsInt=False):
            """Fetch raw data for a single PV.
            Data is delivered to the "callback" callable.
            
            Data covering the interval (T0, Tend] is requested.
            
            If 'count' is provided, then no more than 'count' samples will be
            delivered in totol.
            
            'chunkSize' may be provided as a hint for the number of samples
            buffered before the callback is invoked.
            
            'archs' has the same meaning as with search().

            This function returns a twisted.defer.Deferred which will
            complete with an integer which is the total number of samples
            processed.
            """

        def fetchplot(pv, callback,
                     cbArgs=(), cbKWs={},
                     T0=None, Tend=None,
                     count=None, chunkSize=None,
                     archs=None,
                     enumAsInt=False):
           """Fetch plot binned data.
            Data is delivered to the "callback" callable.
            
            Data covering the interval (T0, Tend] is requested.
            
            'count' must be provided.  However, the number of points returned
            may be more or less.
            
            'chunkSize' may be provided as a hint for the number of samples
            buffered before the callback is invoked.
            
            'archs' has the same meaning as with search().

            This function returns a twisted.defer.Deferred which will
            complete with an integer which is the total number of samples
            processed.
           """
