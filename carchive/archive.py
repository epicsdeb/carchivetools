# -*- coding: utf-8 -*-

from .backend import classic, appl

def getArchive(conf):
    if conf['urltype']=='classic':
        return classic.getArchive(conf)
    elif conf['urltype']=='appl':
        return appl.getArchive(conf)
