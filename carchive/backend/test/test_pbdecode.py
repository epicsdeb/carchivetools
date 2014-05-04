# -*- coding: utf-8 -*-

from unittest import TestCase

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

class TestDecode(TestCase):
    def test_double(self):
        S = EPICSEvent_pb2.ScalarDouble()

        S.val = 42.2
        S.secondsintoyear = 1024
        S.nano = 0x1234

        raw = S.SerializeToString()

        V, M = pbdecode.decode_double([raw])

        self.assertEqual(V, [42.2])
        self.assertEqual(M, [(0, 0, 1024, 0x1234)])
