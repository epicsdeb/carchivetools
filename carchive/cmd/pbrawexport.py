# -*- coding: utf-8 -*-

from __future__ import print_function
import re
import os
from twisted.internet import defer
from carchive.date import makeTimeInterval, makeTime
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import granularity, filepath, escape

class PbExportError(Exception):
    pass

class Exporter(object):
    def __init__ (self, pv_name, out_stream):
        self._pv_name = pv_name
        self._out_stream = out_stream
        self._dtype = None
        self._is_waveform = None
    
    def __call__ (self, data, meta):
        # Get data type of chunk.
        dtype = data.dtype
        is_waveform = data.shape[1] != 1
        
        if self._dtype is None:
            print('first chunk')
            # Remember data type of first chunk.
            self._dtype = dtype
            self._is_waveform = is_waveform
        else:
            print('next chunk')
            # Check data type.
            if dtype != self._dtype or is_waveform != self._is_waveform:
                raise PbExportError('Unexpected data type!')
        
        # Iterate samples in chunk.
        for (i, m) in enumerate(meta):
            # Get value - make it not an array if we have a waveform.
            value = data[i] if is_waveform else data[i][0]


@defer.inlineCallbacks
def cmd(archive=None, opt=None, args=None, conf=None, **kws):
    archs=set()
    for ar in opt.archive:
        archs|=set(archive.archives(pattern=ar))
    archs=list(archs)
    
    # Get out dir.
    if opt.export_out_dir is None:
        raise PbExportError('Output directory not specified!')
    out_dir = opt.export_out_dir
    
    # Get granularity.
    if opt.export_granularity is None:
        raise PbExportError('Export granularity not specified!')
    gran = granularity.get_granularity(opt.export_granularity)
    if gran is None:
        raise PbExportError('Export granularity is not understood!')
    
    # Collect PV name delimiters.
    delimiters = ([] if opt.export_no_default_delimiters else [':', '-']) + \
        ([] if opt.export_delimiter is None else opt.export_delimiter)
    
    # Collect PV name patterns.
    patterns = []
    if opt.export_all:
        patterns.append('.*')
    if opt.export_pattern is not None:
        patterns += opt.export_pattern
    
    # Collect PVs to archive...
    pvs = set()
    
    # Query PVs for patterns.
    for pattern in patterns:
        print('Querying pattern: {}'.format(pattern))
        search_result = yield archive.search(pattern=pattern, archs=archs, breakDown=opt.verbose>1)
        print('--> {} PVs.'.format(len(search_result)))
        pvs.update(search_result)

    # Add explicit PVs.
    pvs.update(args)
    
    # Sort PVs.
    pvs = sorted(pvs)
    
    # Check we have any PVs.
    if len(pvs)==0:
        raise PbExportError('Have no PV names to archive!')
    
    # Resolve time interval.
    T0, Tend = makeTimeInterval(opt.start, opt.end)
    
    # Print some info.
    print('Time range: {} -> {}'.format(T0, Tend))
    print('PVs: {}'.format(' '.join(pvs)))
    
    # Archive PVs one by one.
    for pv in pvs:
        print('PV: {}'.format(pv))
        
        # Get the segment where the start time falls.
        segment = gran.get_segment_for_time(T0)
        
        while True:
            # Calculate the next segment.
            next_segment = segment.next_segment()
            
            # Get the time interval for this segment.
            segment_start_time = segment.start_time()
            segment_end_time = next_segment.start_time()
            
            # Stop if we've already covered the desired time interval.
            if segment_start_time > Tend:
                break
            
            # Don't query outside the desired interval...
            query_start_time = max(segment_start_time, T0)
            query_end_time = min(segment_end_time, Tend)
            
            # Determine the path of the output file.
            out_file_path = filepath.get_path_for_suffix(out_dir, delimiters, pv, segment.file_suffix())
            
            print('[ {} - {} ) --> {}'.format(query_start_time, query_end_time, out_file_path))
            
            # Make sure the file doesn't already exist. There's a race but whatever.
            #if os.path.isfile(out_file_path):
            #    raise PbExportError('Output file already exists!')
            
            # Open the file for writing.
            with open(out_file_path, 'wb') as file_handle:
                # Create exporter.
                exporter = Exporter(pv, file_handle)
                
                # Ask for samples for this interval.
                # This function interprets the interval as half-open (].
                segment_data = yield archive.fetchraw(pv, exporter, archs=archs, cbArgs=(), T0=query_start_time, Tend=query_end_time, chunkSize=opt.chunk, enumAsInt=opt.enumAsInt)
                
                # Process these samples.
                sample_count = yield segment_data
            
            # Continue with the next segment.
            segment = next_segment
    
    print('All done.')
    
    defer.returnValue(0)
