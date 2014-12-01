from __future__ import print_function
import datetime
from carchive.backend import EPICSEvent_pb2 as pbt
from carchive.backend.pb import escape as pb_escape
from carchive.backend.pb import filepath as pb_filepath
from carchive.backend.pb import verify as pb_verify

class AppenderError(Exception):
    pass

class Appender(object):
    def __init__(self, pv_name, gran, out_dir, delimiters, ignore_ts_start, pvlog):
        self._pv_name = pv_name
        self._gran = gran
        self._out_dir = out_dir
        self._delimiters = delimiters
        self._ignore_ts_start = ignore_ts_start
        self._pvlog = pvlog
        
        # Start with no file open.
        self._cur_file = None
        self._cur_start = None
        self._cur_end = None
        self._cur_path = None
    
    def close(self):
        # Close any file we have open.
        if self._cur_file is not None:
            self._cur_file.close()
    
    def write_sample(self, sample_pb, dt_seconds, nanoseconds, pb_type):
        ''' Determines the appropriate file for the sample (based on the timestamp) and 
        writes the given sample into a file.'''  
        # Extract the number of seconds into the year. This should be exact.
        td = (dt_seconds - datetime.datetime(dt_seconds.year, 1, 1))
        into_year_sec_fp = (td.seconds + td.days * 24 * 3600)
        into_year_sec = int(into_year_sec_fp)
        
        # Ignore sample if requested by the lower bound.
        if self._ignore_ts_start is not None:
            if (dt_seconds.year, into_year_sec, nanoseconds) <= self._ignore_ts_start:
                self._pvlog.ignored_initial_sample()
                return
        
        # Write timestamp to sample.
        sample_pb.secondsintoyear = into_year_sec
        sample_pb.nano = nanoseconds
        
        # Serialize sample.
        sample_serialized = sample_pb.SerializeToString()
        
        # If this sample does not belong to the currently opened file, close the file.
        # Note that it's ok to use dt_seconds here since we don't support sub-second granularity.
        # Same goes for the get_segment_for_time call below.
        if self._cur_file is not None and not (self._cur_start <= dt_seconds < self._cur_end):
            self._cur_file.close()
            self._cur_file = None
        
        # Need to open a file?
        if self._cur_file is None:
            # Determine the segment for this sample.
            segment = self._gran.get_segment_for_time(dt_seconds)
            self._cur_start = segment.start_time()
            self._cur_end = segment.next_segment().start_time()
            
            # Sanity check the segment bounds.
            assert (self._cur_start <= dt_seconds < self._cur_end)
            
            # Determine the path of the file.
            self._cur_path = pb_filepath.get_path_for_suffix(self._out_dir, self._delimiters, self._pv_name, segment.file_suffix())
            
            self._pvlog.info('File: {0}'.format(self._cur_path))
            
            # Open file. This creates the file if it does not exist,
            # and the the cursor is set to the *end*.
            self._cur_file = open(self._cur_path, 'a+b')
            
            # Seek to the beginning.
            self._cur_file.seek(0, 0)
            
            # We fail if we found samples newer than this one in the file.
            upper_ts_bound = (into_year_sec, nanoseconds)
            
            # Verify any existing contents of the file.
            try:
                pb_verify.verify_stream(self._cur_file, pb_type=pb_type, pv_name=self._pv_name, year=dt_seconds.year, upper_ts_bound=upper_ts_bound)
                
            except pb_verify.VerificationError as e:
                raise AppenderError('Verification failed: {0}: {1}'.format(self._cur_path, e))
            
            except pb_verify.EmptyFileError:
                # Build header.
                header_pb = pbt.PayloadInfo()
                header_pb.type = pb_type
                header_pb.pvname = self._pv_name
                header_pb.year = dt_seconds.year
                
                # Write header. Note that since there was no header we are still at the start of the file.
                self._cur_file.write(pb_escape.escape_line(header_pb.SerializeToString()))
        
        # Finally write the sample.
        self._cur_file.write(pb_escape.escape_line(sample_serialized))
        
        self._pvlog.archived_sample()
