# -*- coding: utf-8 -*-

import logging
_log = logging.getLogger(__name__)

try:
    from .backend import classic
except ImportError:
    if _log.isEnabledFor(logging.DEBUG):
        _log.exception("Failed to import classic backend")
    classic=None
try:
    from .backend import appl
except ImportError:
    if _log.isEnabledFor(logging.DEBUG):
        _log.exception("Failed to import appliance backend")
    appl=None

def getArchive(conf):
    if conf['urltype']=='classic' and classic:
        return classic.getArchive(conf)
    elif conf['urltype']=='appl' and appl:
        return appl.getArchive(conf)
    raise ValueError("Unsupported urltype: %s"%conf['urltype'])
