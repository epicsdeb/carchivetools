# -*- coding: utf-8 -*-

from unittest import TestCase

import numpy

from ...dtype import dbr_time
from .. import EPICSEvent_pb2, pbdecode

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
        #('byte', EPICSEvent_pb2.ScalarByte, pbdecode.decode_byte, numpy.int8, ['a', 'b']),
        ('short', EPICSEvent_pb2.ScalarShort, pbdecode.decode_short, numpy.int16, [512, 513]),
        ('int', EPICSEvent_pb2.ScalarInt, pbdecode.decode_int, numpy.int32, [0x12345, 0x54321]),
        ('string', EPICSEvent_pb2.ScalarString, pbdecode.decode_string, numpy.dtype('a40'), ["hello", "world"]),
        ('float', EPICSEvent_pb2.ScalarFloat, pbdecode.decode_float, numpy.float32, [42.5, 43.5]),
        ('double', EPICSEvent_pb2.ScalarDouble, pbdecode.decode_double, numpy.float64, [42.1, 42.2]),
    ]

    def test_decode(self):
        
        for L, PB, decode, dtype, vals in self._vals:
            try:
                S = PB()
                raw = []
    
                for i,V in enumerate(vals):
                    S.val = V
                    S.secondsintoyear = 1024+i
                    S.nano = 0x1234+i
                    raw.append(S.SerializeToString())
    
                V = numpy.ndarray((len(vals),1), dtype=dtype)
                M = numpy.ndarray((len(vals),), dtype=dbr_time)
    
                decode(raw, V, M)
                
                for i,eV in enumerate(vals):
                    self.assertEqual(V[i], eV)
                    self.assertEqual(tuple(M[i]), (0, 0, 1024+i, 0x1234+i))
            except:
                print 'Error in test_decode for',L
                raise

    def test_char(self):
        S = EPICSEvent_pb2.ScalarByte()

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

        pbdecode.decode_byte(raw, V, M)

        self.assertEqual(V[0], ord('a'))
        self.assertEqual(tuple(M[0]), (0, 0, 1024, 0x1234))
        self.assertEqual(V[1], ord('b'))
        self.assertEqual(tuple(M[1]), (0, 0, 1025, 0x1235))

    def test_fail(self):
        S = EPICSEvent_pb2.ScalarByte()
        S.val = 'a'
        S.secondsintoyear = 1024
        S.nano = 0x1234

        raw = S.SerializeToString()

        V = numpy.ndarray((2,), dtype=numpy.int8)
        M = numpy.ndarray((2,), dtype=dbr_time)

        # wrong number of list elements
        self.assertRaises(ValueError, pbdecode.decode_byte, [''], V, M)

        # wrong number of meta elements
        self.assertRaises(ValueError, pbdecode.decode_byte, ['',''], V, M[:1])

        # wrong number of value elements
        self.assertRaises(ValueError, pbdecode.decode_byte, ['',''], V[:1], M)

        # wrong value dtype
        self.assertRaises(ValueError, pbdecode.decode_byte, ['',''],
                          numpy.ndarray((2,), dtype=numpy.int16), M)

        # decode empty string
        self.assertRaises(ValueError, pbdecode.decode_byte, ['',''], V, M)

        # decode partial string
        self.assertRaises(ValueError, pbdecode.decode_byte, [raw[:5],''], V, M)

        # decode partial string in second item
        self.assertRaises(ValueError, pbdecode.decode_byte, [raw,raw[:5]], V, M)
