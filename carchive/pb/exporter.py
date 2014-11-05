from __future__ import print_function
import numpy as np
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import escape as pb_escape
from carchive.pb import timestamp as pb_timestamp
from carchive.pb import dtypes as pb_dtypes

class SkipPvError(Exception):
    pass

class Exporter(object):
    def __init__(self, pv_name, year, out_stream):
        self._pv_name = pv_name
        self._year = year
        self._out_stream = out_stream
        self._orig_type = None
        self._is_waveform = None
        self._last_meta = {}
        self._last_meta_day = -1
        self._meta_dirty = True
    
    def __call__(self, data, meta_vec, extraMeta):
        # Get data type of chunk.
        orig_type = extraMeta['orig_type']
        is_waveform = extraMeta['reported_arr_size'] != 1
        
        if self._orig_type is None:
            # Remember data type of first chunk.
            self._orig_type = orig_type
            self._is_waveform = is_waveform
            self._type_desc = pb_dtypes.get_type_description(orig_type)
            
            print('Data type: {}, is_waveform={}'.format(self._type_desc.__name__, self._is_waveform))
            
            if extraMeta['the_meta']['type'] == 0:
                print('WARNING: {}: Enum labels will not be stored.'.format(self._pv_name))
            
            if self._waveform_size_bad(data, extraMeta):
                print('WARNING: {}: Inconsistent waveform size, not archiving this PV.'.format(self._pv_name))
                raise SkipPvError()
            
            # Write header.
            self._write_header()
            
        else:
            # Check data type, it should be the same as received with the first sample.
            if orig_type != self._orig_type or is_waveform != self._is_waveform:
                raise TypeError('Unexpected data type!')
            
            if self._waveform_size_bad(data, extraMeta):
                print('WARNING: {}: Inconsistent waveform size, not archiving this PV anymore (we did manage to archive something).'.format(self._pv_name))
                raise SkipPvError()
        
        # Deal with the metadata.
        new_meta = dict((META_MAP[meta_name], meta_val) for (meta_name, meta_val) in extraMeta['the_meta'].iteritems() if meta_name in META_MAP)
        if new_meta != self._last_meta:
            self._last_meta = new_meta
            self._meta_dirty = True
        
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
        
        # Force metadata on new day (unless there are no samples for a day...).
        sample_day = into_year_sec // 86400
        if sample_day != self._last_meta_day:
            self._meta_dirty = True
        
        # Flush metadata.
        if self._meta_dirty:
            self._meta_dirty = False
            self._last_meta_day = sample_day
            for meta_name in sorted(self._last_meta):
                try:
                    val = convert_meta(self._last_meta[meta_name])
                except TypeError as e:
                    print('WARNING: Could not encode metadata field {}={}: {}'.format(meta_name, repr(self._last_meta[meta_name]), e))
                else:
                    sample_pb.fieldvalues.extend([pbt.FieldValue(name=meta_name, val=val)])
            print('Attaching metadata: {}'.format(', '.join('{}={}'.format(x.name, x.val) for x in sample_pb.fieldvalues)))
        
        # Write it.
        self._write_line(sample_pb.SerializeToString())
    
    def _waveform_size_bad(self, data, extraMeta):
        return self._is_waveform and data.shape[1] != extraMeta['reported_arr_size']

META_MAP = {
    'units': 'EGU',
    'prec': 'PREC',
    'alarm_low': 'LOLO',
    'alarm_high': 'HIHI',
    'warn_low': 'LOW',
    'warn_high': 'HIGH',
    'disp_low': 'LOPR',
    'disp_high': 'HOPR',
}

META_CONVERSION = {
    str: lambda x: x,
    int: lambda x: str(x),
    float: lambda x: '{:.17E}'.format(x) # TBD: We may want to reproduce Java's toString(double)
}

def convert_meta(x):
    if type(x) not in META_CONVERSION:
        raise TypeError('Unsupported metadata type')
    return META_CONVERSION[type(x)](x)
