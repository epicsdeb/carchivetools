# -*- coding: utf-8 -*-
"""
EPICS Alarm Status codes
"""

__all__ = ['get_status']

status = {
    0: '',
    1: 'READ',
    2: 'WRITE',
    3: 'HIHI',
    4: 'HIGH',
    5: 'LOLO',
    6: 'LOW',
    7: 'STATE',
    8: 'COS',
    9: 'COMM',
    10: 'TIMEOUT',
    11: 'HW_LIMIT',
    12: 'CALC',
    13: 'SCAN',
    14: 'LINK',
    15: 'SOFT',
    16: 'BAD_SUB',
    17: 'UDF',
    18: 'DISABLE',
    19: 'SIMM',
    20: 'READ_ACCESS',
    21: 'WRITE_ACCESS',
}

def get_status(code):
    try:
        return status[code]
    except KeyError:
        return 'stat%d'%code
