"""
This software is Copyright by the
 Board of Trustees of Michigan
 State University (c) Copyright 2015.
"""
from __future__ import print_function
import math, datetime
from carchive.backend import EPICSEvent_pb2 as pbt
from carchive.backend.pb import dtypes as pb_dtypes
from carchive.backend.pb import appender as pb_appender

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
        self._previous_dt_seconds = None
        self._previous_nano = None
        self._pv_disconnected = False
        self._print_mysql = True
        self._last_timestamp_secnano = None
        self._prev_severity = 0;
        self._appender = pb_appender.Appender(pv_name, gran, out_dir, delimiters, ignore_ts_start, pvlog)
    
    # with statement entry
    def __enter__(self):
        return self
    
    # with statement exit - clean up
    def __exit__(self, tyype, value, traceback):
        self._pvlog.info('Finished exporting data for PV {0}'.format(self._pv_name))
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
            #if extraMeta['the_meta']['type'] == 0:
            #    self._pvlog.warning('Enum labels will not be stored.')
            
            # The old Channel Archiver cannot handle arrays of strings, so skip these broken PVs.
            if self._waveform_size_bad(data, extraMeta):
                raise SkipPvError('Inconsistent waveform size.')
            
        else:
            # Check data type, it should be the same as received with the first sample.
            if orig_type != self._orig_type:
                if self._orig_type == 2 and orig_type == 1:
                    #if the type changes from an integer type to an enum type, do not complain, just continue storing the samples as ints
                    self._orig_type = orig_type
                    self._pvlog.warning("Data changed from int to enum type. Data will continue to be stored as int.")
                else:
                    raise SkipPvError('Inconsitent data type in subsequent chunk! Previous={0}, new={1}'.format(self._orig_type, orig_type))
            if is_waveform != self._is_waveform:
                raise SkipPvError('Inconsitent waveformness in subsequent chunk!')
            
            if self._waveform_size_bad(data, extraMeta):
                raise SkipPvError('Inconsistent waveform size (we did manage to archive something)')
        
        # If metadata has changed, we will attach it to the first sample in this chunk.
        new_meta = dict((META_MAP[meta_name], meta_val) for (meta_name, meta_val) in extraMeta['the_meta'].items() if meta_name in META_MAP)
        
        if new_meta.has_key('states') and new_meta['states'] is not None:
            new_meta['states'] = ';'.join(new_meta['states'])
        if new_meta != self._last_meta:
            self._last_meta = new_meta
            self._meta_dirty = True
        
        # Iterate samples in chunk.
        for (i, meta) in enumerate(meta_vec):
            # Convert value to a standard format.
            value = list(data[i]) if is_waveform else data[i][0]
            
            # Process the sample.
            self._process_sample(value, int(meta[0]), int(meta[1]), int(meta[2]), int(meta[3]))
                    
        if self._mysql_writer is not None and self._print_mysql:
            self._print_mysql = False
            the_meta = extraMeta['the_meta']
            pv_type =  pb_dtypes.get_pv_type(orig_type,self._is_waveform)
            try:
                self._mysql_writer.put_pv_info(self._pv_name, the_meta['disp_high'], the_meta['disp_low'],
                                             the_meta['alarm_high'], the_meta['alarm_low'],
                                             the_meta['warn_high'], the_meta['warn_low'],
                                             the_meta['disp_high'], the_meta['disp_low'],
                                             the_meta['prec'], the_meta['units'], 
                                             not self._is_waveform, array_size,
                                             pv_type)
            except:
                ''' If we have a PV that doesn't have the limits, do not try to load them '''
                self._mysql_writer.put_pv_info(name=self._pv_name, 
                                             scalar=not self._is_waveform, ncount=array_size,
                                             pv_type=pv_type)
    
    def _process_sample(self, value, sevr, stat, secs, nano):
        #print('sample VAL={} SEVR={} STAT={} SECS={} NANO={}'.format(value, sevr, stat, secs, nano))

        # Build a datetime for the whole seconds.
        # Track nanoseconds separately to avoid time conversion errors.
        # dt_seconds must be in UTC
        dt_seconds = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=secs)
        
        # Skip out-of-order samples in input.
        secnano = (secs, nano)
        if self._last_timestamp_secnano is not None and secnano < self._last_timestamp_secnano:
            self._pvlog.error('Out-of-order sample: last={0} this={1}'.format(self._last_timestamp_secnano, secnano))
        self._last_timestamp_secnano = secnano
        
        # Build sample structure. But leave the time to the appender.
        sample_pb = self._pb_class()
        if self._is_waveform:
            self._type_desc.encode_vector(value, sample_pb)
        else:
            self._type_desc.encode_scalar(value, sample_pb)
        sample_pb.severity = sevr
        sample_pb.status = stat
        #if the severity is 'Disconnected(3904)' skip writing this sample and write it later
        if sevr == 3904 or sevr == 3848 or sevr == 3872:
            if self._pv_disconnected is False:
                self._previous_dt_seconds = secs
                self._previous_nano = nano
            self._pv_disconnected = True
            #if the severity is archive off or archive disabled, store severity, so that we can add 
            #extra fields later when a healthy sample arrives             
            if (sevr == 3848 or sevr == 3872) and self._prev_severity < 4:
                self._prev_severity = sevr
            #samples with severity 3904, 3848 and 3872 are not stored: the info they provide is used
            #with the next healthy value
            return
        elif sevr > 3:
            #sevr == 3856 or sevr == 3968:
            #if the severity is Repeat or Est_Repeat, log a warning
            self._pvlog.warning("Severity {0} encountered at {1}!".format(sevr,secnano))
        else:
            if self._pv_disconnected is True:
                sample_pb.fieldvalues.extend([pbt.FieldValue(name='cnxlostepsecs', val='{0}'.format(self._previous_dt_seconds))])
                sample_pb.fieldvalues.extend([pbt.FieldValue(name='cnxregainedepsecs', val='{0}'.format(secs))])
                if self._prev_severity == 3872:
                    sample_pb.fieldvalues.extend([pbt.FieldValue(name='startup', val='true')])
                elif self._prev_severity == 3848:
                    sample_pb.fieldvalues.extend([pbt.FieldValue(name='resume', val='true')])
                self._prev_severity = sevr
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
    
META_MAP = {
    'units': 'EGU',
    'prec': 'PREC',
    'alarm_low': 'LOLO',
    'alarm_high': 'HIHI',
    'warn_low': 'LOW',
    'warn_high': 'HIGH',
    'disp_low': 'LOPR',
    'disp_high': 'HOPR',
    'states': 'states'
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
