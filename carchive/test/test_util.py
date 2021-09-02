# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

from twisted.internet import defer, error, protocol
from twisted.trial import unittest
from twisted.test import proto_helpers
from twisted.python import failure
#defer.Deferred.debug=1

from .. import util

class MockBLP(util.BufferingLineProtocol):
    def __init__(self, *args, **kws):
        util.BufferingLineProtocol.__init__(self, *args, **kws)
        self.lines = None
        self._procD = defer.Deferred()
    def processLines(self, lines, prev=None):
        assert self.lines is None
        self.lines, self.prev = lines, prev
        return self._procD
    def _finish(self, ok=None, err=None):
        self.lines = None
        D, self._procD = self._procD, defer.Deferred()
        assert not D.called
        if err:
            D.errback(err)
        else:
            D.callback(ok)

class TestBLP(unittest.TestCase):
    timeout = 2

    def setUp(self):
        self.alldone = False
    def tearDown(self):
        self.assertTrue(self.alldone)

    @defer.inlineCallbacks
    def test_empty(self):
        """Receive nothing
        """
        T = proto_helpers.StringTransport()
        P = MockBLP()
        P.makeConnection(T)
        self.assertFalse(P.defer.called)

        P.connectionLost(protocol.connectionDone)
        self.assertTrue(P.defer.called)

        V = yield P.defer
        self.assertIdentical(V, None)
        self.alldone = True

    @defer.inlineCallbacks
    def test_short(self):
        """Receive a short message, less than the buffer size.
           Process when connection closed
        """
        T = proto_helpers.StringTransport()
        P = MockBLP()
        P.makeConnection(T)

        P.dataReceived('A\nB\nC\n')
        self.assertIdentical(P.lines, None) # buffer below limit, no proc.

        P.connectionLost(protocol.connectionDone)
        self.assertEqual(P.lines, ['A','B','C']) # proc. triggered on close
        self.assertFalse(P.defer.called) # not called until processing completes

        ret = object()
        P._finish(ok=ret) # complete processing

        self.assertTrue(P.defer.called) # all done now

        V = yield P.defer
        self.assertIdentical(V, ret)
        self.alldone = True

    @defer.inlineCallbacks
    def test_long(self):
        """Receive two message segments, longer than buffer size
        """
        T = proto_helpers.StringTransport()
        P = MockBLP()
        P.rx_buf_size = 4
        P.makeConnection(T)

        P.dataReceived('A\nB\nC\nD')
        self.assertEqual(P.lines, ['A','B','C']) # first proc. triggered
        self.assertTrue(P.active)

        P.dataReceived('\nE\nF\n')
        self.assertEqual(P.lines, ['A','B','C']) # first proc. still in progress
        self.assertFalse(P.active)

        P._finish() # finish first
        P.dataReceived('G\n') # more data to trigger proc. #2

        self.assertEqual(P.lines, ['D','E','F','G']) # second results
        self.assertFalse(P.defer.called) # not done yet
        
        ret = object()
        P._finish(ok=ret) # finish second
        P.connectionLost(protocol.connectionDone) # close connection

        self.assertTrue(P.defer.called)

        V = yield P.defer
        self.assertIdentical(V, ret)
        self.alldone = True

    @defer.inlineCallbacks
    def test_proc_err(self):
        """Processing function throws exception
        """
        T = proto_helpers.StringTransport()
        P = MockBLP()
        P.rx_buf_size = 4
        P.makeConnection(T)

        P.dataReceived('A\nB\nC\n')
        self.assertEqual(P.lines, ['A','B','C'])

        P._finish(err=RuntimeError('oops'))
        self.assertEqual(T.producerState, 'stopped')
        P.connectionLost(error.ConnectionClosed())

        # connection error will be ignored in favor of proc. error
        P.connectionLost(failure.Failure(error.ConnectionAborted()))

        try:
            yield P.defer
            self.assertTrue(False)
        except RuntimeError as e:
            self.assertEqual(e.message, 'oops')

        self.alldone = True

    @defer.inlineCallbacks
    def test_badline_err(self):
        """protocol completes with incomplete line
        """
        T = proto_helpers.StringTransportWithDisconnection()
        P = MockBLP()
        P.rx_buf_size = 4
        T.protocol = P
        P.makeConnection(T)

        P.dataReceived('A\nB\nC') # trailing '\n' for last line never sent
    
        self.assertEqual(P.lines, ['A','B'])

        P.connectionLost(protocol.connectionDone) # normal close
        self.assertFalse(P._procD.called)
        self.assertFalse(P.defer.called) # not called until processing completes

        P._finish(ok=42)

        self.assertTrue(P.defer.called)

        try:
            V = yield P.defer
            self.assertEqual(V, 42) # should never get here
            self.assertTrue(False, 'missing expected exception')
        except RuntimeError as e:
            self.assertEqual(e.message, 'connection closed with partial line')

        self.alldone = True

    @defer.inlineCallbacks
    def test_conn_err(self):
        """Connection closes with data in buffer, but no proc.
        """
        T = proto_helpers.StringTransportWithDisconnection()
        P = MockBLP()
        T.protocol = P
        P.makeConnection(T)

        P.dataReceived('A\nB\nC\n')
        self.assertIdentical(P.lines, None)

        P.connectionLost(failure.Failure(error.ConnectionAborted()))

        try:
            yield P.defer
            self.assertTrue(False)
        except error.ConnectionAborted:
            pass

        self.alldone = True

    @defer.inlineCallbacks
    def test_conn2_err(self):
        """Connection closes with data proc. in progress
        """
        T = proto_helpers.StringTransportWithDisconnection()
        P = MockBLP()
        P.rx_buf_size = 4
        T.protocol = P
        P.makeConnection(T)

        P.dataReceived('A\nB\nC')
        self.assertEqual(P.lines, ['A','B'])

        P.connectionLost(failure.Failure(error.ConnectionAborted()))
        self.assertFalse(P.defer.called) # not called until processing completes

        P._finish(ok=42)

        try:
            yield P.defer
            self.assertTrue(False)
        except error.ConnectionAborted:
            pass

        self.alldone = True
