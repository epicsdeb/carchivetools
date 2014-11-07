from __future__ import print_function
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import escape as pb_escape
from carchive.pb import filepath as pb_filepath
from carchive.pb import verify as pb_verify

class AppenderError(Exception):
    pass

class Appender(object):
    def __init__(self, pv_name, gran, out_dir, delimiters):
        self._pv_name = pv_name
        self._gran = gran
        self._out_dir = out_dir
        self._delimiters = delimiters
        
        # Start with no file open.
        self._cur_file = None
        self._cur_start = None
        self._cur_end = None
        self._cur_path = None
    
    def close(self):
        # Close any file we have open.
        if self._cur_file is not None:
            self._cur_file.close()
    
    def write_sample(self, sample_serialized, the_datetime, into_year_sec, into_year_nsec, pb_type):
        # If this sample does not belong to the currently opened file, close the file.
        if self._cur_file is not None and not (the_datetime >= self._cur_start and the_datetime < self._cur_end):
            self._cur_file.close()
            self._cur_file = None
        
        # Need to open new file?
        if self._cur_file is None:
            # Determine the segment for this sample.
            segment = self._gran.get_segment_for_time(the_datetime)
            self._cur_start = segment.start_time()
            self._cur_end = segment.next_segment().start_time()
            
            # Determine the path of the file.
            self._cur_path = pb_filepath.get_path_for_suffix(self._out_dir, self._delimiters, self._pv_name, segment.file_suffix())
            
            print('-- File: {}'.format(self._cur_path))
            
            # Open file. This creates the file if it does not exist,
            # and the the cursor is set to the *end*.
            self._cur_file = open(self._cur_path, 'a+b')
            
            # Seek to the beginning.
            self._cur_file.seek(0, 0)
            
            # We fail if we found samples newer than this one in the file.
            upper_ts_bound = (into_year_sec, into_year_nsec)
            
            # Verify any existing contents of the file.
            try:
                pb_verify.verify_stream(self._cur_file, pb_type, self._pv_name, the_datetime.year, upper_ts_bound)
                
            except pb_verify.VerificationError as e:
                raise AppenderError('Verification failed: {}: {}'.format(self._cur_path, e))
            
            except pb_verify.EmptyFileError:
                # Build header.
                header_pb = pbt.PayloadInfo()
                header_pb.type = pb_type
                header_pb.pvname = self._pv_name
                header_pb.year = the_datetime.year
                
                # Write header. Note that since there was no header we are still at the start of the file.
                self._cur_file.write(pb_escape.escape_line(header_pb.SerializeToString()))
        
        # Finally write the sample.
        self._cur_file.write(pb_escape.escape_line(sample_serialized))
 