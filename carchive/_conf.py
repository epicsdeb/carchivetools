# -*- coding: utf-8 -*-
"""
Internal module
"""

def __loadConfig():
    import os.path
    import ConfigParser
    dflt={'url':'http://%(host)s/cgi-bin/ArchiveDataServer.cgi',
          'host':'%%(host)s',
          'defaultarchs':'*/Current',
        }
    cf=ConfigParser.SafeConfigParser(defaults=dflt)
    cf.read([
        '/etc/carchive.conf',
        os.path.expanduser('~/.carchiverc'),
        'carchive.conf'
    ])
    if not cf.has_section('_unspecified_'):
        cf.add_section('_unspecified_') # to view DEFAULT
    return cf
_conf=__loadConfig()
