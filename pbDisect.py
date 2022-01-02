#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.

Utility to help with debugging protobuf decode failures

Processes arbitrary PB messages.  Extra information may be injected with --type
to enable sub-structures to be decoded.

eg. operate on line 1 of testdata.pb generically

  ./pbDisect.py --file carchive/backend/test/testdata.pb 1

or specifically as PayloadInfo

  ./pbDisect.py --file carchive/backend/test/testdata.pb 1 --type PayloadInfo
"""

from __future__ import print_function

import struct, re

def getargs():
    import argparse
    P = argparse.ArgumentParser()
    P.add_argument('--sample', action='store_const', dest='proc', const=sample)
    P.add_argument('--file', action='store_const', dest='proc', const=dfile)
    P.add_argument('-S','--show', action='store_true')
    P.add_argument('--test', action='store_const', dest='proc', const=dtest)
    P.add_argument('--type', dest='pbtype', default='Generic')
    P.add_argument('-U','--unescape', action='store_true')
    P.add_argument('input', nargs='*')
    return P.parse_args()

_unesc = re.compile(rb'\x1b(.)')
_unmap = {
    b'\x01': b'\x1b',
    b'\x02': b'\x0a',
    b'\x03': b'\x0d',
}
def _unfn(M):
    return _unmap[M.group(1)]
def unescape(inp):
    return _unesc.sub(_unfn, inp.strip())

def wrap(I):
    for C in I:
        print('>>',repr(C))
        yield C

def decodeVI(B):
    """
    >>> decodeVI(iter('\x01'))
    1
    >>> decodeVI(iter('\xac\x02'))
    300
    """
    val, n = 0, 0
    while True:
        x = next(B)
        val += (x&0x7f)<<(n*7)
        if not x&0x80:
            return val
        n+=1

def decodeString(B):
    L = decodeVI(B)
    print('  Length:',L)
    V = [None]*L
    for n in range(L):
        V[n] = next(B)
    return bytes(V)

def showVI(B):
    val = decodeVI(B)
    print('  Value:',val)

def showV64(B):
    V = bytes([next(B) for i in range(8)])
    print('  Value:',repr(V),struct.unpack('<d',V)[0])

def showString(B):
    V = decodeString(B)
    print('  Value (%d): %s'%(len(V),repr(V)))

def showStart(B):
    print('Nested start')

def showEnd(B):
    print('Nested start')

def showV32(B):
    V = ''
    for i in range(4):
        V+=next(B)
    print('  Value:',repr(V),struct.unpack('<f',V)[0])

wirename = {
  0:'Varint',
  1:'64-bit',
  2:'Length',
  3:'Start ',
  4:'End   ',
  5:'32-bit',
}

wiredecode = {
  0:showVI,
  1:showV64,
  2:showString,
  3:showStart,
  4:showEnd,
  5:showV32,
}

# Provide type-specific information for additional decoding.
# Index by field
PBTypes = {
    'Generic':{},
    'PayloadInfo':{15:('struct','FieldValue')},
    'AAValue':{7:('struct','FieldValue')},
    'FieldValue':{},
    'Example':{3:{}}, # from PB documentation
}

def decode(B, Ks, fn=iter):
    try:
        while True:
            id = next(B)
            # except a message key
            wire = id&0x7
            idx = id>>3
            print('Index',idx,wirename[wire])
            if idx in Ks and wire==2:
                L = decodeVI(B)
                print(' bytes',L)
                sub = bytes([next(B) for n in range(L)])

                info = Ks[idx]
                if info[0]=='struct':
                    print(' Sub ->')
                    decode(fn(sub), PBTypes[info[1]])
                    print(' Sub <-')

                elif info[0]=='packed':
                    sB = fn(sub)
                    while True:
                        info[1](sB)

            else:
                wiredecode[wire](B)

    except StopIteration:
        pass

def sample(args):
    import ast
    fn=iter
    if args.show:
        fn=wrap

    for I in args.input:
        I = ast.literal_eval("b'%s'"%I)
        print('Input',repr(I))
        try:
            decode(fn(I), PBTypes[args.pbtype])
        except:
            import traceback
            traceback.print_exc()

def dfile(args):
    fn=iter
    if args.show:
        fn=wrap
    fname = args.input[0]
    ln = int(args.input[1] or '1')
    with open(fname, 'rb') as F:
        for i,L in enumerate(F,1):
            if i==ln:
                I = unescape(L)
                print('Input',repr(I))
                try:
                    decode(fn(I), PBTypes[args.pbtype])
                except:
                    import traceback
                    traceback.print_exc()

def dtest(args):
    import doctest
    doctest.testmod()

def main(args):
    args.proc(args)

if __name__=='__main__':
    main(getargs())
