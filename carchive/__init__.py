# -*- coding: utf-8 -*-
"""
Archiver XMLRPC client

@author: Michael Davidsaver <mdavidsaver@bnl.gov>
"""

__all__ = ['__version__',
           'getArchive']

import logging

from archive import getArchive, HandledError

__version__ = 'pre2'

if not hasattr(logging, 'NullHandler'):
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
else:
    NullHandler = logging.NullHandler

__h=NullHandler()
logging.getLogger("carchive").addHandler(__h)
