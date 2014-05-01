#!/usr/bin/env python

from distutils.core import setup, Extension

from numpy.distutils.misc_util import get_numpy_include_dirs

setup(
    name = "carchivetools",
    version = "1.0",
    description = "CLI Tools to query Channel Archiver",
    author = "Michael Davidsaver",
    author_email = "mdavidsaver@bnl.gov",
    license = "BSD",
    packages = ['carchive', 'carchive.cmd'],
    scripts = ['arget','arplothdf5'],
    ext_modules=[Extension('carchive.backend.pbdecode',
                           ['carchive/backend/pbdecode.cpp',
                            'carchive/backend/EPICSEvent.pb.cc'],
                           include_dirs=get_numpy_include_dirs(),
                           libraries=['protobuf'],
                 )],
)
