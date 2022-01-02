"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""

import sys
from .. import date, util, _conf

__doctests__ = [util, _conf]
if sys.version_info>=(3,0):
    # TODO: differences in datetime.__repr__ make doctest compatibility difficult
    #       should rewrite to unittest
    __doctests__.append(date)
