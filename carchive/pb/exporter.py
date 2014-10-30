from __future__ import print_function
import numpy as np
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import escape as pb_escape
from carchive.pb import timestamp as pb_timestamp
from carchive.pb import dtypes as pb_dtypes

class Exporter(object):
    def __init__(self, pv_name, year, out_stream):
        self._pv_name = pv_name
        self._year = year
        self._out_stream = out_stream
        self._orig_type = None
        self._is_waveform = None
    
    def __call__(self, data, meta_vec, extraMeta):
        # Get data type of chunk.
        orig_type = extraMeta['orig_type']
        is_waveform = data.shape[1] != 1
        
        if self._orig_type is None:
            # Remember data type of first chunk.
            self._orig_type = orig_type
            self._is_waveform = is_waveform
            self._type_desc = pb_dtypes.get_type_description(orig_type)
            
            print('Data type: {}, is_waveform={}'.format(self._type_desc.__name__, self._is_waveform))
            
            # Write header.
            self._write_header()
            
        else:
            # Check data type, it should be the same as received with the first sample.
            if orig_type != self._orig_type or is_waveform != self._is_waveform:
                raise TypeError('Unexpected data type!')
        
        # Iterate samples in chunk.
        for (i, meta) in enumerate(meta_vec):
            # Convert value to a standard format.
            value = list(data[i]) if is_waveform else data[i][0]
            
            # Write the sample to the output stream.
            self._write_sample(value, int(meta[0]), int(meta[1]), int(meta[2]), int(meta[3]))
    
    def _write_line(self, data):
        # Write to output stream, escsaped and with a newline.
        self._out_stream.write(pb_escape.escape_line(data) + pb_escape.NEWLINE_CHAR)
    
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
        year, into_year_sec, into_year_nsec = pb_timestamp.carchive_to_aapb(secs, nano)
        
        # The year should be the the one we have in the header.
        if year != self._year:
            raise ValueError('Incorrect year received in sample!')
        
        # Build sample structure.
        sample_pb = self._type_desc.PB_CLASS[self._is_waveform]()
        sample_pb.secondsintoyear = into_year_sec
        sample_pb.nano = into_year_nsec
        if self._is_waveform:
            self._type_desc.encode_vector(value, sample_pb)
        else:
            self._type_desc.encode_scalar(value, sample_pb)
        sample_pb.severity = sevr
        sample_pb.status = stat
        
        # Write it.
        self._write_line(sample_pb.SerializeToString())