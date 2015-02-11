# -*- coding: utf-8 -*-

import os.path

from twisted.trial import unittest

from twisted.internet import defer, error, protocol
from twisted.test import proto_helpers

import numpy as np
from numpy.testing import assert_array_almost_equal

from .. import appl
from ...dtype import dbr_time

# Read in an example PB message stream
with open(os.path.join(os.path.dirname(__file__), 'testdata.pb')) as F:
    _data = F.read()
del F

_values = [np.asarray([[ 0.03],[ 2.17],[ 0.45],[-0.15],[-0.31],[-0.21],
       [-0.14],[-0.08],[-0.02],[ 0.04],[ 0.02]], dtype=np.float64),
       np.asarray([[ 0.  ],[ 2.18],[ 0.44],[-0.14],[-0.32],[-0.26],[-0.21],
       [-0.14],[-0.09],[-0.03],[ 0.03]], dtype=np.float64),
]
_metas = [np.rec.array([(0L, 0, 1423234604L, 887015782L), (0L, 0, 1423248954L, 139922833L),
       (0L, 0, 1423248955L, 140245250L), (0L, 0, 1423248956L, 140024882L),
       (0L, 0, 1423248957L, 140228286L), (0L, 0, 1423248961L, 145268115L),
       (0L, 0, 1423248963L, 145419813L), (0L, 0, 1423248965L, 145170191L),
       (0L, 0, 1423248969L, 145384148L), (0L, 0, 1423249758L, 541449008L),
       (0L, 0, 1423250956L, 140990782L)], dtype=dbr_time),
       np.rec.array([(3904L, 0, 1423250956L, 0L), (0L, 0, 1423263362L, 434265082L),
       (0L, 0, 1423263363L, 429269655L), (0L, 0, 1423263364L, 434134740L),
       (0L, 0, 1423263365L, 434277492L), (0L, 0, 1423263368L, 434441414L),
       (0L, 0, 1423263369L, 434220574L), (0L, 0, 1423263371L, 434272868L),
       (0L, 0, 1423263373L, 434366836L), (0L, 0, 1423263377L, 439388932L),
       (0L, 0, 1423263404L, 449503115L)], dtype=dbr_time),
]

_all_values = np.concatenate(_values, axis=0)
_all_metas = np.concatenate(_metas, axis=0)

class CB(object):
    def __init__(self):
        self.data = []
    def __call__(self, *args):
        self.data.append(args)

class TestApplST(unittest.TestCase):
    timeout = 1
    inthread = False

    def setUp(self):
        self.alldone = False
        self.cb = cb = CB()
        self.P = appl.PBReceiver(cb, name='LN-AM{RadMon:1}DoseRate-I',
                                 inthread=self.inthread)
        self.T = proto_helpers.StringTransport()

    def tearDown(self):
        self.assertTrue(self.alldone)

    @defer.inlineCallbacks
    def test_decodeall(self):
        """Receive the entire reply at once
        """
        self.P.makeConnection(self.T)
        self.P.dataReceived(_data)

        self.assertFalse(self.P.defer.called)
        self.P.connectionLost(protocol.connectionDone)
        if not self.inthread:
            self.assertTrue(self.P.defer.called)

        C = yield self.P.defer
        self.assertEqual(C,22)

        self.assertEqual(len(self.cb.data), 2)

        V, M = self.cb.data[0]
        self.assertEqual(V.shape, (11,1))
        assert_array_almost_equal(V, _values[0])
        assert_array_almost_equal(M['severity'], _metas[0]['severity'])
        assert_array_almost_equal(M['status'], _metas[0]['status'])
        assert_array_almost_equal(M['sec'], _metas[0]['sec'])
        assert_array_almost_equal(M['ns'], _metas[0]['ns'])

        V, M = self.cb.data[1]
        self.assertEqual(V.shape, (11,1))
        assert_array_almost_equal(V, _values[1])
        assert_array_almost_equal(M['severity'], _metas[1]['severity'])
        assert_array_almost_equal(M['status'], _metas[1]['status'])
        assert_array_almost_equal(M['sec'], _metas[1]['sec'])
        assert_array_almost_equal(M['ns'], _metas[1]['ns'])

        self.alldone = True

    @defer.inlineCallbacks
    def test_bytebybyte(self):
        """Receive one byte at a time
        """
        self.P.rx_buf_size = 1 # shorten buffer
        self.P.makeConnection(self.T)
        [self.P.dataReceived(B) for B in _data]

        if not self.inthread:
            self.assertEqual(len(self.cb.data), 21)

            self.assertFalse(self.P.defer.called)

        self.P.connectionLost(protocol.connectionDone)

        if not self.inthread:
            self.assertTrue(self.P.defer.called)

        C = yield self.P.defer
        self.assertEqual(C,22)

        if self.inthread:
            # Due to threading, grouping will not be by individual line
            self.assertEqual(len(self.cb.data), 2)
        else:
            self.assertEqual(len(self.cb.data), 21)

        val = np.concatenate([V for V,M in self.cb.data], axis=0)
        meta = np.concatenate([M for V,M in self.cb.data], axis=0)

        self.assertEqual(val.shape, (22,1))

        assert_array_almost_equal(val, _all_values)
        assert_array_almost_equal(meta['severity'], _all_metas['severity'])
        assert_array_almost_equal(meta['status'], _all_metas['status'])
        assert_array_almost_equal(meta['sec'], _all_metas['sec'])
        assert_array_almost_equal(meta['ns'], _all_metas['ns'])

        self.alldone = True

class TestApplMT(TestApplST):
    inthread = True
