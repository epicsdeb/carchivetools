from __future__ import print_function
import numpy as np
from carchive.pb import EPICSEvent_pb2 as pbt

class DoubleTypeDesc(object):
    ORIG_TYPE = 3
    PB_TYPE = (pbt.SCALAR_DOUBLE, pbt.WAVEFORM_DOUBLE)
    PB_CLASS = (pbt.ScalarDouble, pbt.VectorDouble)
    
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
