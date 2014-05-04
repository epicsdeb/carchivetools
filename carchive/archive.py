# -*- coding: utf-8 -*-


try:
    from .backend import classic
except ImportError:
    classic=None
try:
    from .backend import appl
except ImportError:
    appl=None

def getArchive(conf):
    if conf['urltype']=='classic' and classic:
        return classic.getArchive(conf)
    elif conf['urltype']=='appl' and appl:
        return appl.getArchive(conf)
    raise ValueError("Unsupported urltype: %s"%conf['urltype'])
