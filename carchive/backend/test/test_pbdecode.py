# -*- coding: utf-8 -*-

from unittest import TestCase

import numpy

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

                V = numpy.ndarray((len(vals),1), dtype=_dtypes[decode])
                M = numpy.ndarray((len(vals),), dtype=dbr_time)

                I,L = pbdecode.decoders[decode](raw, V, M)
                self.assertTrue(L is None)
                self.assertEqual(I, len(vals))

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

        V = numpy.ndarray((2,1), dtype=numpy.int8)
        M = numpy.ndarray((2,), dtype=dbr_time)

        pbdecode.decode_scalar_byte(raw, V, M)

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

        V = numpy.ndarray((2,), dtype=numpy.int8)
        M = numpy.ndarray((2,), dtype=dbr_time)

        # wrong number of list elements
        self.assertRaises(ValueError, pbdecode.decode_scalar_byte, [''], V, M)

        # wrong number of meta elements
        self.assertRaises(ValueError, pbdecode.decode_scalar_byte, ['',''], V, M[:1])

        # wrong number of value elements
        self.assertRaises(ValueError, pbdecode.decode_scalar_byte, ['',''], V[:1], M)

        # wrong value dtype
        self.assertRaises(ValueError, pbdecode.decode_scalar_byte, ['',''],
                          numpy.ndarray((2,), dtype=numpy.int16), M)

        # decode empty string
        self.assertRaises(pbdecode.DecodeError, pbdecode.decode_scalar_byte, ['',''], V, M)

        # decode partial string
        self.assertRaises(pbdecode.DecodeError, pbdecode.decode_scalar_byte, [raw[:5],''], V, M)

        # decode partial string in second item
        self.assertRaises(pbdecode.DecodeError, pbdecode.decode_scalar_byte, [raw,raw[:5]], V, M)

        try:
            pbdecode.decode_scalar_byte([raw,raw[:5]], V, M)
            self.assertTrue(False, "Should not get here")
        except pbdecode.DecodeError as e:
            self.assertEqual(e.args, (1,))

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

                V = numpy.ndarray((len(vals),1), dtype=_dtypes[decode])
                M = numpy.ndarray((len(vals),), dtype=dbr_time)

                I=0
                while I<len(M):
                    Ix,L = pbdecode.decoders[decode](raw[I:], V[I:], M[I:])
                    I+=Ix
                    assert L is None or I<len(M)
                    if L is not None:
                        assert L>V.shape[1]
                        V.resize((V.shape[0], L))
                    else:
                        assert I==len(M)

                for i,eV in enumerate(vals):
                    self.assertTrue(numpy.all(V[i,:len(eV)]==numpy.asarray(eV, dtype=_dtypes[decode])))
                    #self.assertFalse(numpy.any(V[i,len(eV):]))
                    self.assertEqual(tuple(M[i]), (0, 0, 1024+i, 0x1234+i))
            except:
                print 'Error in test_decode for',name
                raise

    def test_double(self):
        S = _fields[13]()
        S.val.extend([1.1, 2.2, 3.3])
        S.secondsintoyear = 1024
        S.nano = 0x1234

        raw = [S.SerializeToString()]

        V = numpy.ndarray((1,1), dtype=numpy.float64)
        M = numpy.ndarray((1,), dtype=dbr_time)

        I, L = pbdecode.decode_vector_double(raw, V, M)

        self.assertEqual(I, 0)
        self.assertEqual(L, 3)

        V = numpy.ndarray((1,3), dtype=numpy.float64)
        M = numpy.ndarray((1,), dtype=dbr_time)

        I, L = pbdecode.decode_vector_double(raw, V, M)

        self.assertEqual(I, 1)
        self.assertIs(L, None)
        self.assertEqual(list(V[0,:]), [1.1, 2.2, 3.3])

        S.Clear()
        S.val.extend([0.1, 1.1, 2.2, 3.3])
        S.secondsintoyear = 1025
        S.nano = 0x1235

        raw.append(S.SerializeToString())

        V = numpy.ndarray((2,1), dtype=numpy.float64)
        M = numpy.ndarray((2,), dtype=dbr_time)

        I, L = pbdecode.decode_vector_double(raw, V, M)

        self.assertEqual(I, 0)
        self.assertEqual(L, 3)

        V.resize((V.shape[0], L), refcheck=True)

        I, L = pbdecode.decode_vector_double(raw, V, M)

        self.assertEqual(I, 1)
        self.assertEqual(L, 4)

        V.resize((V.shape[0], L), refcheck=True)

        I, L = pbdecode.decode_vector_double(raw, V, M)

        self.assertEqual(I, 2)
        self.assertIs(L, None)
        self.assertEqual(list(V[0,:]), [1.1, 2.2, 3.3, 0.0])
        self.assertEqual(list(V[1,:]), [0.1, 1.1, 2.2, 3.3])

if __name__=='__main__':
    import unittest
    unittest.main()
