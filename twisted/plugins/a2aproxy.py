# -*- coding: utf-8 -*-

import logging

from zope.interface import implements

from twisted.python import usage, log
from twisted.plugin import IPlugin
from twisted.application import service

from twisted.application.internet import TCPServer
from twisted.web.server import Site

try:
    from twisted.manhole.telnet import ShellFactory
except ImportError:
    ShellFactory = None

from carchive.a2aproxy.resource import buildResource

class Log2Twisted(logging.StreamHandler):
    """Print logging module stream to the twisted log
    """
    def __init__(self):
        super(Log2Twisted,self).__init__(stream=self)
        self.write = log.msg
    def flush(self):
        pass

class Options(usage.Options):
    optFlags = [
        ["debug", "d", "Run daemon in developer (noisy) mode"],
    ]
    optParameters = [
        ['ip', '', "", "Address of interface to bind (default all)"],
        ['port', 'P', 8888, "Port to listen on (default 7004)", int],
        ['appl', 'A', "http://localhost:17665/mgmt/bpl/getApplianceInfo", "/getApplianceInfo URL"],
        ['manhole', 'M', 2222, "Manhole port (default not-run)", int],
    ]
    def postOptions(self):
        if self['port'] < 1 or self['port'] > 65535:
            raise usage.UsageError('Port out of range')

class Maker(object):
    implements(service.IServiceMaker, IPlugin)
    tapname = 'a2aproxy'
    description = "Archiver to Appliance proxy"
    options = Options

    def makeService(self, opts):
        L = logging.INFO
        if opts['debug']:
            L = logging.DEBUG

        handle = Log2Twisted()
        handle.setFormatter(logging.Formatter("%(message)s"))
        root = logging.getLogger()
        root.addHandler(handle)
        root.setLevel(L)

        serv = service.MultiService()

        fact = Site(buildResource(opts['appl']))

        serv.addService(TCPServer(opts['port'], fact, interface=opts['ip']))

        if ShellFactory and opts['manhole']:
            print 'Opening Manhole'
            SF = ShellFactory()
            SS = TCPServer(opts['manhole'], SF, interface='127.0.0.1')

            # populate manhole shell locals
            SF.namespace['site'] = fact

            serv.addService(SS)
        else:
            print 'No Manhole'

        return serv

serviceMaker = Maker()
