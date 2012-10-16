# -*- coding: utf-8 -*-
"""
Internal module
"""

def __loadConfig():
    import os.path
    import ConfigParser
    dflt={'url':'http://%(host)s/cgi-bin/ArchiveDataServer.cgi',
          'host':'%%(host)s',
          'defaultarchs':'*',
          'defaultcount':'10',
          'defaultarchive':'DEFAULT',
        }
    cf=ConfigParser.SafeConfigParser(defaults=dflt)
    cf.read([
        '/etc/carchive.conf',
        os.path.expanduser('~/.carchiverc'),
        'carchive.conf'
    ])
    return cf
_conf=__loadConfig()
