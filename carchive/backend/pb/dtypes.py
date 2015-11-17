"""
This software is Copyright by the
 Board of Trustees of Michigan
 State University (c) Copyright 2015.
"""
from __future__ import print_function
from __future__ import absolute_import
from carchive.backend import EPICSEvent_pb2 as pbt

'''
    dtypes.py provides routines to convert the channel archiver data types to archiver appliance data types.
'''

class DoubleTypeDesc(object):
    ORIG_TYPE = 3
    PB_TYPE = (pbt.SCALAR_DOUBLE, pbt.WAVEFORM_DOUBLE)
    PB_CLASS = (pbt.ScalarDouble, pbt.VectorDouble)
    PB_NAME = ('DBR_SCALAR_DOUBLE','DBR_WAVEFORM_DOUBLE')
    NAME = 'Double'
    
    @staticmethod
    def encode_scalar(value, sample_pb):
        sample_pb.val = float(value)
    
    @staticmethod
    def encode_vector(value, sample_pb):
        sample_pb.val.extend(float(x) for x in value)

class Int32TypeDesc(object):
    ORIG_TYPE = 2
    PB_TYPE = (pbt.SCALAR_INT, pbt.WAVEFORM_INT)
    PB_CLASS = (pbt.ScalarInt, pbt.VectorInt)
    PB_NAME = ('DBR_SCALAR_INT','DBR_WAVEFORM_INT')
    NAME = 'Int32'
    
    @staticmethod
    def encode_scalar(value, sample_pb):
        sample_pb.val = int(value)
    
    @staticmethod
    def encode_vector(value, sample_pb):
        sample_pb.val.extend(int(x) for x in value)

class StringTypeDesc(object):
    ORIG_TYPE = 0
    PB_TYPE = (pbt.SCALAR_STRING, pbt.WAVEFORM_STRING)
    PB_CLASS = (pbt.ScalarString, pbt.VectorString)
    PB_NAME = ('DBR_SCALAR_STRING','DBR_WAVEFORM_STRING')
    NAME = 'String'
    
    @staticmethod
    def encode_scalar(value, sample_pb):
        sample_pb.val = str(value)
    
    @staticmethod
    def encode_vector(value, sample_pb):
        sample_pb.val.extend(str(x) for x in value)

class EnumTypeDesc(object):
    ORIG_TYPE = 1
    PB_TYPE = (pbt.SCALAR_ENUM, pbt.WAVEFORM_ENUM)
    PB_CLASS = (pbt.ScalarEnum, pbt.VectorEnum)
    PB_NAME = ('DBR_SCALAR_ENUM','DBR_WAVEFORM_ENUM')
    NAME = 'Enum'
    
    @staticmethod
    def encode_scalar(value, sample_pb):
        sample_pb.val = int(value)
    
    @staticmethod
    def encode_vector(value, sample_pb):
        sample_pb.val.extend(int(x) for x in value)

_desc = [DoubleTypeDesc, Int32TypeDesc, StringTypeDesc, EnumTypeDesc]

_ALL_CLASS_DESCRIPTIONS, _ALL_TYPE_DESCRIPTIONS = {}, {}
for T in _desc:
    _ALL_CLASS_DESCRIPTIONS[T.PB_TYPE[0]] = T.PB_CLASS[0]
    _ALL_CLASS_DESCRIPTIONS[T.PB_TYPE[1]] = T.PB_CLASS[1]
    _ALL_TYPE_DESCRIPTIONS[T.ORIG_TYPE] = T

def get_type_description(orig_type):
    desc = _ALL_TYPE_DESCRIPTIONS[orig_type]
    if desc == None:
        raise TypeError('Got unsupported data type.')
    return desc

def get_pv_type(orig_type, is_waveform):
    desc = _ALL_TYPE_DESCRIPTIONS[orig_type]
    if desc == None:
        raise TypeError('Unsupported data type {0}'.format(orig_type))
    return desc.PB_NAME[1] if is_waveform else desc.PB_NAME[0]

class UnknownPbTypeError(Exception):
    pass

def get_pb_class_for_type(pb_type):
    clazz = _ALL_CLASS_DESCRIPTIONS[pb_type]
    if clazz == None:
        raise UnknownPbTypeError('Unknown PB type')
    return clazz
