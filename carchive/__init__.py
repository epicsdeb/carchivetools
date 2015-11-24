# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.

Archiver XMLRPC client

@author: Michael Davidsaver <mdavidsaver@bnl.gov>
"""
from __future__ import absolute_import

__all__ = ['__version__',
          ]

import logging

__version__ = '2.2'
version = (2,2)

if not hasattr(logging, 'NullHandler'):
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
else:
    NullHandler = logging.NullHandler

__h=NullHandler()
logging.getLogger("carchive").addHandler(__h)
