# -*- coding: utf-8 -*-
"""
Archiver XMLRPC client

@author: Michael Davidsaver <mdavidsaver@bnl.gov>
"""

__all__ = ['__version__',
           'Archiver']

import logging

from query import ArchiveQuery
from archiver import Archiver

__version__ = 'pre1'

if not hasattr(logging, 'NullHandler'):
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

__h=NullHandler()
logging.getLogger("carchive").addHandler(__h)
