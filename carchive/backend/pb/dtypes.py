"""
This software is Copyright by the
 Board of Trustees of Michigan
 State University (c) Copyright 2015.
"""
from __future__ import print_function
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

ALL_TYPE_DESCRIPTIONS = [DoubleTypeDesc, Int32TypeDesc, StringTypeDesc, EnumTypeDesc]

def get_type_description(orig_type):
    for type_desc in ALL_TYPE_DESCRIPTIONS:
        if orig_type == type_desc.ORIG_TYPE:
            return type_desc
    raise TypeError('Got unsupported data type.')

def get_pv_type(orig_type, is_waveform):
    for type_desc in ALL_TYPE_DESCRIPTIONS:
        if orig_type == type_desc.ORIG_TYPE:
            return type_desc.PB_NAME[1] if is_waveform else type_desc.PB_NAME[0] 
    raise TypeError('Unsupported data type {0}'.format(orig_type))

class UnknownPbTypeError(Exception):
    pass

def get_pb_class_for_type(pb_type):
    for type_desc in ALL_TYPE_DESCRIPTIONS:
        if type_desc.PB_TYPE[0] == pb_type:
            return type_desc.PB_CLASS[0]
        if type_desc.PB_TYPE[1] == pb_type:
            return type_desc.PB_CLASS[1]
    raise UnknownPbTypeError('Unknown PB type')
