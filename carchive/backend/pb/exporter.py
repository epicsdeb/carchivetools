from __future__ import print_function
import math
from carchive.backend import EPICSEvent_pb2 as pbt
from carchive.backend.pb import dtypes as pb_dtypes
from carchive.backend.pb import appender as pb_appender
from carchive.date import makeTime

class SkipPvError(Exception):
    pass

class Exporter(object):
    def __init__(self, pv_name, gran, out_dir, delimiters, ignore_ts_start, pvlog, mysql_writer=None):
        self._pv_name = pv_name
        self._pvlog = pvlog
        self._mysql_writer = mysql_writer
        self._orig_type = None
        self._is_waveform = None
        self._type_desc = None
        self._pb_type = None
        self._last_meta = {}
        self._last_meta_day = None
        self._meta_dirty = True
        self._previous_disconnected_event = None
        self._previous_dt_seconds = None
        self._previous_nano = None
        self._pv_disconnected = False
        self._appender = pb_appender.Appender(pv_name, gran, out_dir, delimiters, ignore_ts_start, pvlog)
    
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
        array_size = extraMeta['reported_arr_size']
        is_waveform = array_size != 1
        
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
            
            self._pvlog.info('Data type: {0} {1}'.format(self._type_desc.NAME, ('Waveform' if self._is_waveform else 'Scalar')))
            
            # We do get enum labels but we cannot store them anywhere :(
            if extraMeta['the_meta']['type'] == 0:
                self._pvlog.warning('Enum labels will not be stored.')
            
            # The old Channel Archiver cannot handle arrays of strings, so skip these broken PVs.
            if self._waveform_size_bad(data, extraMeta):
                raise SkipPvError('Inconsistent waveform size.')
            
        else:
            # Check data type, it should be the same as received with the first sample.
            if orig_type != self._orig_type:
                raise SkipPvError('Inconsitent data type in subsequent chunk!')
            if is_waveform != self._is_waveform:
                raise SkipPvError('Inconsitent waveformness in subsequent chunk!')
            
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
                    
        if self._mysql_writer is not None:
            the_meta = extraMeta['the_meta']
            pv_type =  pb_dtypes.get_pv_type(orig_type,self._is_waveform)
            self._mysql_writer.put_pv_info(self._pv_name, the_meta['disp_high'], the_meta['disp_low'],
                                             the_meta['alarm_high'], the_meta['alarm_low'],
                                             the_meta['warn_high'], the_meta['warn_low'],
                                             the_meta['disp_high'], the_meta['disp_low'],
                                             the_meta['prec'], the_meta['units'], 
                                             not self._is_waveform, array_size,
                                             pv_type)
        
    def write_last_disconnected(self):
        ''' If the last event that was pulled from the archive marks the PV as disconnected, it has
        not been written yet, because extra fields have to be added to the sample. Add those fields
        to the sample and write it. In addition, mark the disconnected state in the mysql writer.'''
        if self._previous_disconnected_event is not None:
            self._write_previous()
        if self._pv_disconnected:
            self._mysql_writer.pv_disconnected(self._pv_name)
            self._pv_disconnected = False
    
    def _process_sample(self, value, sevr, stat, secs, nano):
        #print('sample VAL={} SEVR={} STAT={} SECS={} NANO={}'.format(value, sevr, stat, secs, nano))

        # Build a datetime for the whole seconds.
        # Track nanoseconds separately to avoid time conversion errors.
        dt_seconds = makeTime(secs)
        #datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=secs)
        
        # Build sample structure. But leave the time to the appender.
        sample_pb = self._pb_class()
        if self._is_waveform:
            self._type_desc.encode_vector(value, sample_pb)
        else:
            self._type_desc.encode_scalar(value, sample_pb)
        sample_pb.severity = sevr
        sample_pb.status = stat
        #if the severity is 'Disconnected(3904)' skip writing this sample and write it later
        if sevr == 3904:
            if self._previous_disconnected_event is None:
                sample_pb.fieldvalues.extend([pbt.FieldValue(name='cnxlostepsecs', val='{0}'.format(secs))])
                sample_pb.severity = 0
                sample_pb.status = 0
                self._previous_disconnected_event = sample_pb
                self._previous_dt_seconds = dt_seconds
                self._previous_nano = nano
            self._pv_disconnected = True
            return
        elif sevr == 3872 or sevr == 3848:
            self._pv_disconnected = True
        else:
            if self._previous_disconnected_event is not None:
                self._previous_disconnected_event.fieldvalues.extend([pbt.FieldValue(name='cnxregainedepsecs', val='{0}'.format(secs))])
                self._write_previous()
            self._pv_disconnected = False
        
        # Force metadata on new day (unless there are no samples for a day...).
        sample_day = dt_seconds.date()
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
                    self._pvlog.warning('Could not encode metadata field {0}={1}: {2}'.format(meta_name, repr(self._last_meta[meta_name]), e))
                else:
                    sample_pb.fieldvalues.extend([pbt.FieldValue(name=meta_name, val=val)])
        
        # Write it via the appender.
        try:
            self._appender.write_sample(sample_pb, dt_seconds, nano, self._pb_type)
        except pb_appender.AppenderError as e:
            raise SkipPvError(e)
    
    def _waveform_size_bad(self, data, extraMeta):
        return self._is_waveform and data.shape[1] != extraMeta['reported_arr_size']
    
    def _write_previous(self):
        ''' Write the previous disconnected event. The event has to exist. '''
        try:
            self._appender.write_sample(self._previous_disconnected_event, self._previous_dt_seconds, self._previous_nano, self._pb_type)
        except pb_appender.AppenderError as e:
            raise SkipPvError(e)
        self._previous_disconnected_event = None

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

def meta_convert_float(x):
    # For non-finite values, match the behavior of Java's toString(double).
    # For finite values, use exponential notation.
    if math.isnan(x):
        return 'NaN'
    if math.isinf(x):
        return 'Infinity' if x > 0.0 else '-Infinity'
    return '{0}'.format(x)
    #return '{:.17E}'.format(x)

META_CONVERSION = {
    str: lambda x: x,
    int: lambda x: str(x),
    float: lambda x: meta_convert_float(x)
}

def convert_meta(x):
    if type(x) not in META_CONVERSION:
        raise TypeError('Unsupported metadata type')
    return META_CONVERSION[type(x)](x)
