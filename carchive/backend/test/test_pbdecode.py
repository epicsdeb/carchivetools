# -*- coding: utf-8 -*-

import sys

from unittest import TestCase
from twisted.trial.unittest import SkipTest

if sys.version_info<(2,7):
    raise SkipTest('Not supported for python 2.6')

import numpy
from numpy.testing import assert_equal

from ...dtype import dbr_time
from .. import pbdecode
from ..appl import _dtypes

from .. import EPICSEvent_pb2 as pb

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

class TestEscape(TestCase):
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
                print 'Error in test_decode for',L
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

        # decode empty string
        self.assertRaises(pbdecode.DecodeError, pbdecode.decode_scalar_byte, ['',''], 1)

        # decode partial string
        self.assertRaises(pbdecode.DecodeError, pbdecode.decode_scalar_byte, [raw[:5],''], 1)

        # decode partial string in second item
        self.assertRaises(pbdecode.DecodeError, pbdecode.decode_scalar_byte, [raw,raw[:5]], 1)

        try:
            pbdecode.decode_scalar_byte([raw,raw[:5]], 1)
            self.assertTrue(False, "Should not get here")
        except pbdecode.DecodeError as e:
            self.assertEqual(e.args, (raw[:5],))

    def test_disconn(self):
        S = _fields[6]()
        S.val = 1.0
        S.severity = 1
        S.secondsintoyear = 1024
        S.nano = 0x1234

        raw = [S.SerializeToString()]
        
        S.Clear()
        S.val = 2.0
        S.severity = 2
        S.secondsintoyear = 1025
        S.nano = 0x1234
        S.fieldvalues.add(name='cnxlostepsecs',val='5678')

        raw.append(S.SerializeToString())

        V, M = pbdecode.decoders[6](raw, 1)
        M = numpy.rec.array(M, dtype=dbr_time)

        assert_equal(M['severity'], [1, 3904])

        V, M = pbdecode.decoders[6](raw, 0)
        M = numpy.rec.array(M, dtype=dbr_time)

        assert_equal(M['severity'], [1, 3904, 2])
        assert_equal(M['sec'], [1024, 1025, 1025])
        assert_equal(M['ns'], [4660, 4659, 4660])

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
                print 'Error in test_decode for',name
                raise

if __name__=='__main__':
    import unittest
    unittest.main()
