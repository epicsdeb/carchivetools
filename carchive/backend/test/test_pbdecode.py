# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

from __future__ import print_function

import sys, logging

from unittest import TestCase
from twisted.trial.unittest import SkipTest

if sys.version_info<(2,7):
    raise SkipTest('Not supported for python 2.6')

import numpy
from numpy.testing import assert_equal, assert_array_equal

from ...dtype import dbr_time
from .. import pbdecode
from ..appl import _dtypes

from .. import EPICSEvent_pb2 as pb

from google.protobuf.message import DecodeError as PyDecodeError

# Proto buffer instances for decoding individual samples
_fields = {
    0:pb.ScalarString,
    1:pb.ScalarShort,
    2:pb.ScalarFloat,
    3:pb.ScalarEnum,
    4:pb.ScalarByte,
    5:pb.ScalarInt,
    6:pb.ScalarDouble,
    7:pb.VectorString,
    8:pb.VectorShort,
    9:pb.VectorFloat,
    10:pb.VectorEnum,
    #11:pb.VectorByte, # missing?
    12:pb.VectorInt,
    13:pb.VectorDouble,
    14:pb.V4GenericBytes,
}

class CaptureHandler(logging.Handler):
    def __init__(self, *args):
        logging.Handler.__init__(self, *args)
        self.logs = []
    def handle(self, rec):
        self.logs.append(rec.getMessage())

class TestUnEscape(TestCase):
    def test_noop(self):
        self.assertEqual('', pbdecode.unescape(''))
        self.assertEqual('hello', pbdecode.unescape('hello'))

    def test_unescape(self):
        self.assertEqual('\x1b', pbdecode.unescape('\x1b\x01'))
        self.assertEqual('\n', pbdecode.unescape('\x1b\x02'))
        self.assertEqual('\r', pbdecode.unescape('\x1b\x03'))

        self.assertEqual('\n\n', pbdecode.unescape('\x1b\x02\x1b\x02'))

        self.assertEqual('Testin \ng', pbdecode.unescape('Testin \x1b\x02g'))

        self.assertEqual('Test\nin \ng',
                         pbdecode.unescape('Test\x1b\x02in \x1b\x02g'))

    def test_fail(self):
        self.assertRaises(ValueError, pbdecode.unescape, '\x1b' )

        self.assertRaises(ValueError, pbdecode.unescape, '\x1b\x1b' )

        self.assertRaises(ValueError, pbdecode.unescape, 'hello \x1bworld' )

        self.assertRaises(ValueError, pbdecode.unescape, 'hello \x1b' )
        self.assertRaises(ValueError, pbdecode.unescape, '\x1bworld' )

class TestEscape(TestCase):
    def test_noop(self):
        self.assertEqual('', pbdecode.escape(''))
        self.assertEqual('hello', pbdecode.escape('hello'))

    def test_escape(self):
        self.assertEqual('\x1b\x01', pbdecode.escape('\x1b'))
        self.assertEqual('\x1b\x02', pbdecode.escape('\n'))
        self.assertEqual('\x1b\x03', pbdecode.escape('\r'))

        self.assertEqual('\x1b\x02\x1b\x02', pbdecode.escape('\n\n'))

        self.assertEqual('Testin \x1b\x02g', pbdecode.escape('Testin \ng'))

        self.assertEqual('Test\x1b\x02in \x1b\x02g',
                         pbdecode.escape('Test\nin \ng'))

class TestSplitter(TestCase):
    def test_split(self):
        fn = pbdecode.linesplitter
        self.assertEqual(fn([]), [[]])
        self.assertEqual(fn(['']), [[],None,[]])
        self.assertEqual(fn(['a']), [['a']])
        self.assertEqual(fn(['a','b']), [['a','b']])
        self.assertEqual(fn(['a','b','','c']), [['a','b'],None,['c']])
        self.assertEqual(fn(['','a','c','','b']), [[],None, ['a','c'],None ,['b']])

class TestDecodeScalar(TestCase):
    _vals = [
        ('short', 1, [512, 513]),
        ('int', 5, [0x12345, 0x54321]),
        ('string', 0, ["hello", "world"]),
        ('float', 2, [42.5, 43.5]),
        ('double', 6, [42.1, 42.2]),
    ]

    def test_decode(self):

        for L, decode, vals in self._vals:
            try:
                S = _fields[decode]()
                raw = []

                for i,V in enumerate(vals):
                    S.Clear()
                    S.val = V
                    S.secondsintoyear = 1024+i
                    S.nano = 0x1234+i
                    raw.append(S.SerializeToString())

                V, M = pbdecode.decoders[decode](raw, 1)
                M = numpy.rec.array(M, dtype=dbr_time)

                self.assertEqual(V.shape[0], len(vals))
                self.assertEqual(V.shape[1], 1)
                self.assertEqual(M.shape[0], len(vals))

                for i,eV in enumerate(vals):
                    self.assertEqual(V[i], eV)
                    self.assertEqual(tuple(M[i]), (0, 0, 1024+i, 0x1234+i))
            except:
                print('Error in test_decode for',L)
                raise

    def test_char(self):
        S = _fields[4]()

        raw = [None]*2

        S.val = 'a'
        S.secondsintoyear = 1024
        S.nano = 0x1234

        raw[0] = S.SerializeToString()

        S.val = 'b'
        S.secondsintoyear = 1025
        S.nano = 0x1235

        raw[1] = S.SerializeToString()

        V, M = pbdecode.decode_scalar_byte(raw, 1)
        M = numpy.rec.array(M, dtype=dbr_time)

        self.assertEqual(V[0], ord('a'))
        self.assertEqual(tuple(M[0]), (0, 0, 1024, 0x1234))
        self.assertEqual(V[1], ord('b'))
        self.assertEqual(tuple(M[1]), (0, 0, 1025, 0x1235))

    def test_fail(self):
        S = _fields[4]()
        S.val = 'a'
        S.secondsintoyear = 1024
        S.nano = 0x1234

        raw = S.SerializeToString()

        # wrong type
        self.assertRaises(TypeError, pbdecode.decode_scalar_byte, [1], 1)
        self.assertRaises(TypeError, pbdecode.decode_scalar_byte, [raw,4], 1)

        H = CaptureHandler()
        L = pbdecode._getLogger()
        L.addHandler(H)

        # decode empty string
        V, M = pbdecode.decode_scalar_byte(['',''], 1)
        M = numpy.rec.array(M, dtype=dbr_time)

        self.assertEqual(V.shape, (2,1))
        self.assertEqual(M.shape, (2,))
        assert_array_equal(M['severity'], [103,103])

        # decode partial string
        V, M = pbdecode.decode_scalar_byte([raw[:5],''], 1)
        M = numpy.rec.array(M, dtype=dbr_time)

        self.assertEqual(V.shape, (2,1))
        self.assertEqual(M.shape, (2,))
        assert_array_equal(M['severity'], [103,103])

        # decode partial string in second item
        V, M = pbdecode.decode_scalar_byte([raw,raw[:5]], 1)
        M = numpy.rec.array(M, dtype=dbr_time)

        self.assertEqual(V.shape, (2,1))
        self.assertEqual(M.shape, (2,))
        assert_array_equal(M['severity'], [0,103])

        L.removeHandler(H)

        # all three decodes result in the same error, so only one message
        self.assertEqual(len(H.logs), 8)
        self.assertRegex(H.logs[0], 'missing required fields:')
        self.assertRegex(H.logs[1], 'protobuf decode fails:')
        self.assertRegex(H.logs[2], 'missing required fields:')
        self.assertRegex(H.logs[3], 'protobuf decode fails:')
        self.assertRegex(H.logs[4], 'protobuf decode fails:')
        self.assertRegex(H.logs[5], 'missing required fields:')
        self.assertRegex(H.logs[6], 'protobuf decode fails:')
        self.assertRegex(H.logs[7], 'protobuf decode fails:')

    _dis_data = [
        # easy case, disconnect has different second
        ((5,1500), (15,750000), 10, (10,0)),
        # Disconnect has same second as previous sample
        ((5,1500), (15,750000), 5, (5,1501)),
        # Disconnect has same second as previous sample, at the second boundary
        ((5,999999999), (15,750000), 5, (6,0)),
        # Disconnect has same second as next sample
        ((5,1500), (15,750000), 15, (15,749999)),
        # Disconnect has same second as next sample, at the second boundary
        ((5,1500), (15,0), 15, (14,999999999)),
        # Disconnect has same second as next and previous samples
        ((15,400000), (15,500000), 15, (15,450000)),
        # really pathological case.  No right answer here...
        ((15,400000), (15,400001), 15, (15,400000)),
    ]

    def _dis(self, prevT, nextT, badT, result):
        sectoyear = 100000

        S = _fields[6]()
        S.val = 1.0
        S.severity = 1
        S.secondsintoyear, S.nano = prevT

        raw = [S.SerializeToString()]
        
        S.Clear()
        S.val = 2.0
        S.severity = 2
        S.secondsintoyear, S.nano = nextT
        S.fieldvalues.add(name='cnxlostepsecs',val=str(badT+sectoyear))

        raw.append(S.SerializeToString())

        V, M = pbdecode.decoders[6](raw, 1)
        M = numpy.rec.array(M, dtype=dbr_time)

        assert_equal(M['severity'], [1, 3904])

        V, M = pbdecode.decoders[6](raw, 0, sectoyear)
        M = numpy.rec.array(M, dtype=dbr_time)

        assert_equal(M['severity'], [1, 3904, 2])
        assert_equal(M['sec'], [prevT[0], result[0], nextT[0]])
        assert_equal(M['ns'], [prevT[1], result[1], nextT[1]])

    def test_disconn(self):
        for D in self._dis_data:
            try:
                self._dis(*D)
            except:
                print('Failure with', D)
                raise

class TestDecodeVector(TestCase):
    _vals = [
        ('short', 8,
             [[512, 513],[514, 515, 516]]),
        ('int', 12,
             [[0x12345, 0x54321], [0x12345, 0x54321, 0x21312]]),
        ('string', 7,
             [["hello", "world"],["This","is","a test"]]),
        ('float', 9,
             [[42.5, 43.5], [45.5, 46.5, 47.5]]),
        ('double', 13,
             [[42.5, 43.5], [45.5, 46.5, 47.5]]),
    ]

    def test_decode(self):

        for name, decode, vals in self._vals:
            try:
                S = _fields[decode]()
                raw = []

                for i,V in enumerate(vals):
                    S.Clear()
                    S.val.extend(V)
                    S.secondsintoyear = 1024+i
                    S.nano = 0x1234+i
                    raw.append(S.SerializeToString())

                V, M = pbdecode.decoders[decode](raw, 1)
                M = numpy.rec.array(M, dtype=dbr_time)

                self.assertEqual(V.shape[0], len(vals))
                self.assertEqual(V.shape[1], 3)
                self.assertEqual(M.shape[0], len(vals))

                for i,eV in enumerate(vals):
                    self.assertTrue(numpy.all(V[i,:len(eV)]==numpy.asarray(eV, dtype=_dtypes[decode])))
                    #self.assertFalse(numpy.any(V[i,len(eV):]))
                    self.assertEqual(tuple(M[i]), (0, 0, 1024+i, 0x1234+i))
            except:
                print('Error in test_decode for',name)
                raise

class TestSpecial(TestCase):
    def setUp(self):
        H = self.H = CaptureHandler()
        L = self.L = pbdecode._getLogger()
        L.addHandler(H)
    def tearDown(self):
        self.L.removeHandler(self.H)

    def test_wrongtype(self):
        """Found this sample being returned from a caplotbinning query
        with the wrong type code.
        caplotbinning has since been fixed.
        """
        _data =['\x08\x80\x88\xa4\x01\x10\x00\x19\x00\x00\x00\x00\x00\x00>@']
        I = pb.ScalarInt()
        I.ParseFromString(_data[0]) # Should fail! Really ScalarDouble
        self.assertEqual(I.val, 0)
        self.assertEqual(I.secondsintoyear, 2688000)
        I = pb.ScalarDouble()
        I.ParseFromString(_data[0])
        self.assertEqual(I.val, 30)
        self.assertEqual(I.secondsintoyear, 2688000)

        V, M = pbdecode.decoders[5](_data, 0)
        M = numpy.rec.array(M, dtype=dbr_time)

        self.assertEqual(V.shape, (1,1))
        self.assertEqual(M.shape, (1,))
        assert_array_equal(M['severity'], [103])

        self.assertEqual(len(self.H.logs), 2)
        self.assertRegex(self.H.logs[0], 'missing required fields: val')
        self.assertRegex(self.H.logs[1], 'protobuf decode fails:')

        V, M = pbdecode.decoders[6](_data, 0)
        self.assertEqual(V[0,0], 30)

    def test_invalid2(self):
        """Truncated sample
        """
        _data=['\x08\xf9\x8e\xc3\x01\x10\x80\xfe\x83\x9d\x03\x19']
        I = pb.ScalarDouble()
        self.assertRaises(PyDecodeError, I.ParseFromString, _data[0])

        _data=['\x08\xf9\x8e\xc3\x01\x10\x80\xfe\x83\x9d\x03\x19']
        V, M = pbdecode.decoders[6](_data, 0)
        M = numpy.rec.array(M, dtype=dbr_time)

        self.assertEqual(V.shape, (1,1))
        self.assertEqual(M.shape, (1,))
        assert_array_equal(M['severity'], [103])

        # libprotobuf tells us nothing about the cause of the failure...
        self.assertEqual(len(self.H.logs), 1)
        self.assertRegex(self.H.logs[0], 'protobuf decode fails:')

if __name__=='__main__':
    import unittest
    unittest.main()
