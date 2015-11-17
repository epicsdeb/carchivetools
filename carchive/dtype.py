# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""
from __future__ import absolute_import

import numpy as np

dbr_time = [
    ('severity', np.uint32),
    ('status', np.uint16),
    ('sec', np.uint32),
    ('ns', np.uint32)
]
