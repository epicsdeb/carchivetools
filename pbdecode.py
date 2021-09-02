#!/usr/bin/env python
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.

Test decoding of PB files, either unprocessed retrieved over http, or on disk files
"""

from __future__ import print_function

import sys, logging

from twisted.internet import defer, error, protocol
from twisted.python.failure import Failure
from twisted.test import proto_helpers

from carchive.backend import appl

logging.basicConfig(level=logging.DEBUG)

def printData(V,M):
  print('cb',V.shape,M.shape)

if len(sys.argv)>1:
  inp=open(sys.argv[1],'rb')
else:
  inp=sys.stdin
if len(sys.argv)>2:
  size=int(sys.argv[2])
else:
  size=4096

P=appl.PBReceiver(printData, name=inp.name, inthread=False)
T=proto_helpers.StringTransport()
P.makeConnection(T)

while True:
  D = inp.read(size)
  if not D:
    print('File consumed')
    break
  P.dataReceived(D)
  if P.defer.called:
    print('Done early',inp.tell())
    break

print('Flush')
P.connectionLost(protocol.connectionDone)

assert P.defer.called

print('Done',P.defer.result)
