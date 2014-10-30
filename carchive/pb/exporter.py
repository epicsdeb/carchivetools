from __future__ import print_function
import datetime
import math
import numpy as np
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import escape

class Exporter(object):
    def __init__(self, pv_name, year, out_stream):
        self._pv_name = pv_name
        self._year = year
        self._out_stream = out_stream
        self._dtype = None
        self._is_waveform = None
    
    def __call__(self, data, meta_vec):
        # Get data type of chunk.
        dtype = data.dtype
        is_waveform = data.shape[1] != 1
        
        if self._dtype is None:
            print('first chunk')
            
            # Remember data type of first chunk.
            self._dtype = dtype
            self._is_waveform = is_waveform
            self._type_desc = get_type_description(dtype)
            
            # Write header.
            self._write_header()
            
        else:
            print('next chunk')
            
            # Check data type, it should be the same as received with the first sample.
            if dtype != self._dtype or is_waveform != self._is_waveform:
                raise TypeError('Unexpected data type!')
        
        # Iterate samples in chunk.
        for (i, meta) in enumerate(meta_vec):
            # Get value - make it not an array if we have a waveform.
            value = data[i] if is_waveform else data[i][0]
            
            # Write the sample to the output stream.
            self._write_sample(value, int(meta[0]), int(meta[1]), int(meta[2]), int(meta[3]))
    
    def _write_line(self, data):
        # Write to output stream, escsaped and with a newline.
        self._out_stream.write(escape.escape_line(data) + '\n')
    
    def _write_header(self):
        # Build header structure.
        header_pb = pbt.PayloadInfo()
        header_pb.type = self._type_desc.PB_TYPE[self._is_waveform]
        header_pb.pvname = self._pv_name
        header_pb.year = self._year
        
        # Write it.
        self._write_line(header_pb.SerializeToString())
    
    def _write_sample(self, value, sevr, stat, secs, nano):
        print('sample VAL={} SEVR={} STAT={} SECS={} NANO={}'.format(value, sevr, stat, secs, nano))
        
        # Convert timestamp.
        year, into_year_sec, into_year_nsec = timestamp_carchive_to_pb(secs, nano)
        
        # The year should be the the one we have in the header.
        if year != self._year:
            raise ValueError('Incorrect year received in sample!')
        
        # Build sample structure.
        sample_pb = self._type_desc.PB_CLASS[self._is_waveform]()
        sample_pb.secondsintoyear = into_year_sec
        sample_pb.nano = into_year_nsec
        sample_pb.val = self._type_desc.encode_vector(value) if self._is_waveform else self._type_desc.encode_scalar(value)
        sample_pb.severity = sevr
        sample_pb.status = stat
        
        # Write it.
        self._write_line(sample_pb.SerializeToString())

def timestamp_carchive_to_pb(input_sec, input_nsec):
    # Channel Archiver time: (secs, nano) - time since 1970 UTC
    # Archiver Appliance time: (year, seconds, nano) - time since year UTC
    input_dt = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=input_sec, microseconds=input_nsec/1000.0)
    year_dt = datetime.datetime(input_dt.year, 1, 1)
    into_year_delta = input_dt - year_dt
    into_year_sec_float = into_year_delta.total_seconds()
    f, i = math.modf(into_year_sec_float)
    into_year_sec = int(i)
    into_year_nsec = min(999999999, int(1e9 * f))
    return (input_dt.year, into_year_sec, into_year_nsec)

def get_type_description(carchive_dtype):
    for type_desc in ALL_TYPE_DESCRIPTIONS:
        if carchive_dtype == type_desc.CARCHIVE_TYPE:
            return type_desc
    raise TypeError('Got unsupported data type.')

class DoubleTypeDesc(object):
    CARCHIVE_TYPE = np.float64
    PB_TYPE = (pbt.SCALAR_DOUBLE, pbt.WAVEFORM_DOUBLE)
    PB_CLASS = (pbt.ScalarDouble, pbt.VectorDouble)
    
    @staticmethod
    def encode_scalar(value):
        return float(value)

ALL_TYPE_DESCRIPTIONS = [DoubleTypeDesc]
