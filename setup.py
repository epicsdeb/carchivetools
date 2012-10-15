#!/usr/bin/env python

from distutils.core import setup

setup(
    name = "carchive",
    version = "1.0",
    description = "CLI Tools to query Channel Archiver",
    author = "Michael Davidsaver",
    author_email = "mdavidsaver@bnl.gov",
    license = "BSD",
    packages = ['carchive'],
    scripts = ['arget'],
)
