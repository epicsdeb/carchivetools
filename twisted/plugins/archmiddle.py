# -*- coding: utf-8 -*-

from __future__ import print_function

import logging

try:
    from configparser import SafeConfigParser as ConfigParser
except ImportError:
    from ConfigParser import SafeConfigParser as ConfigParser

from zope.interface import implementer

from twisted.internet import reactor
from twisted.python import usage, log
from twisted.plugin import IPlugin
from twisted.application import service

from twisted.application.internet import TCPServer
from twisted.web.server import Site

try:
    from twisted.manhole.telnet import ShellFactory
except ImportError:
    ShellFactory = None

class Log2Twisted(logging.StreamHandler):
    """Print logging module stream to the twisted log
    """
    def __init__(self):
        super(Log2Twisted,self).__init__(stream=self)
        self.write = log.msg
    def flush(self):
        pass

class Options(usage.Options):
    optParameters = [
        ['config', 'C', 'archmiddle.conf', 'Config file'],
    ]
    def postOptions(self):
        C = ConfigParser()
        try:
            with open(self['config'], 'r') as F:
                C.readfp(F)
        except (IOError, OSError) as e:
            raise usage.UsageError("Error reading '%s' (%s)"%(self['config'],e))
        self['config'] = C

@implementer(service.IServiceMaker, IPlugin)
class Maker(object):
    tapname = 'archmiddle'
    description = "Channel Archiver middleware"
    options = Options

    def makeService(self, opts):
        from carchive._conf import ConfigDict
        from carchive.archmiddle.proxy import buildResource
        from carchive.archmiddle.info import InfoCache, KeyNameMap

        server = ConfigDict(opts['config'], 'server')
        mapping = ConfigDict(opts['config'], 'mapping')

        handle = Log2Twisted()
        handle.setFormatter(logging.Formatter("%(message)s"))
        root = logging.getLogger()
        root.addHandler(handle)
        root.setLevel(server.getint('log.level', logging.INFO))

        # turn down the noise level...
        CRL = logging.getLogger('carchive.rpcmunge')
        if CRL.isEnabledFor(logging.DEBUG):
            CRL.setLevel(logging.INFO)

        mservice = service.MultiService()

        _M = []
        for k, v in mapping.iteritems():
            v = v.split()
            _M.append((k,int(v[0]), v[1:]))

        KM = KeyNameMap(_M)
        info = InfoCache(server['url'], KM)

        root, leaf = buildResource(info, reactor)
        fact = Site(root)

        info.pvlimit = server.getint('cache.limit', 500)
        info.timeout = server.getfloat('cache.timeout', 3600)

        mservice.addService(TCPServer(server.getint('port'),
                                  fact,
                                  interface=server.get('interface','')))

        if ShellFactory and server.getint('manhole.port', 0):
            print('Opening Manhole')
            SF = ShellFactory()
            SS = TCPServer(server.getint('manhole.port', 0), SF,
                           interface='127.0.0.1')

            # populate manhole shell locals
            SF.namespace['site'] = fact
            SF.namespace['node'] = leaf
            SF.namespace['info'] = info

            mservice.addService(SS)
        else:
            print('No Manhole')

        return mservice

serviceMaker = Maker()
