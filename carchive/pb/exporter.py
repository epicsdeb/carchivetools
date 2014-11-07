from __future__ import print_function
import numpy as np
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import timestamp as pb_timestamp
from carchive.pb import dtypes as pb_dtypes
from carchive.pb import appender as pb_appender

class SkipPvError(Exception):
    pass

class Exporter(object):
    def __init__(self, pv_name, gran, out_dir, delimiters):
        self._pv_name = pv_name
        self._orig_type = None
        self._is_waveform = None
        self._type_desc = None
        self._pb_type = None
        self._last_meta = {}
        self._last_meta_day = -1
        self._meta_dirty = True
        self._appender = pb_appender.Appender(pv_name, gran, out_dir, delimiters)
    
    # with statement entry
    def __enter__(self):
        return self
    
    # with statement exit - clean up
    def __exit__(self, tyype, value, traceback):
        self._appender.close()
    
    # called by fetchraw for every chunk of samples receeived.
    def __call__(self, data, meta_vec, extraMeta):
        # Get data type of chunk.
        orig_type = extraMeta['orig_type']
        is_waveform = extraMeta['reported_arr_size'] != 1
        
        # Got first chunk?
        if self._orig_type is None:
            # Remember data type of first chunk.
            self._orig_type = orig_type
            self._is_waveform = is_waveform
            
            # Find a type description class with type-specific code.
            self._type_desc = pb_dtypes.get_type_description(orig_type)
            
            # Remember the PB type code and class.
            self._pb_type = self._type_desc.PB_TYPE[self._is_waveform]
            self._pb_class = self._type_desc.PB_CLASS[self._is_waveform]
            
            print('Data type: {}, is_waveform={}'.format(self._type_desc.__name__, self._is_waveform))
            
            # We do get enum labels but we cannot store them anywhere :(
            if extraMeta['the_meta']['type'] == 0:
                print('WARNING: {}: Enum labels will not be stored.'.format(self._pv_name))
            
            # The old Channel Archiver cannot handle arrays of strings, so skip these broken PVs.
            if self._waveform_size_bad(data, extraMeta):
                raise SkipPvError('Inconsistent waveform size.')
            
        else:
            # Check data type, it should be the same as received with the first sample.
            if orig_type != self._orig_type:
                raise TypeError('Inconsitent data type in subsequent chunk!')
            if is_waveform != self._is_waveform:
                raise TypeError('Inconsitent waveformness in subsequent chunk!')
            
            if self._waveform_size_bad(data, extraMeta):
                raise SkipPvError('Inconsistent waveform size (we did manage to archive something)')
        
        # If metadata has changed, we will attach it to the first sample in this chunk.
        new_meta = dict((META_MAP[meta_name], meta_val) for (meta_name, meta_val) in extraMeta['the_meta'].iteritems() if meta_name in META_MAP)
        if new_meta != self._last_meta:
            self._last_meta = new_meta
            self._meta_dirty = True
        
        # Iterate samples in chunk.
        for (i, meta) in enumerate(meta_vec):
            # Convert value to a standard format.
            value = list(data[i]) if is_waveform else data[i][0]
            
            # Process the sample.
            self._process_sample(value, int(meta[0]), int(meta[1]), int(meta[2]), int(meta[3]))
    
    def _process_sample(self, value, sevr, stat, secs, nano):
        print('sample VAL={} SEVR={} STAT={} SECS={} NANO={}'.format(value, sevr, stat, secs, nano))
        
        # Convert timestamp.
        the_datetime, into_year_sec, into_year_nsec = pb_timestamp.carchive_to_aapb(secs, nano)
        
        # Build sample structure.
        sample_pb = self._pb_class()
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
        
        # Serialize it.
        sample_serialized = sample_pb.SerializeToString()
        
        # Write it via the appender.
        try:
            self._appender.write_sample(sample_serialized, the_datetime, into_year_sec, into_year_nsec, self._pb_type, self._pb_class)
        except pb_appender.AppenderError as e:
            raise SkipPvError(e)
    
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
