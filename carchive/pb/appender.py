from __future__ import print_function
import os
import google.protobuf as protobuf
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import escape as pb_escape
from carchive.pb import filepath as pb_filepath

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
    
    def write_sample(self, sample_serialized, the_datetime, into_year_sec, into_year_nsec, pb_type, pb_class):
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
            
            # Prepare line iterator.
            line_iterator = pb_escape.iter_lines(self._cur_file)
            
            # Check if we have a header.
            try:
                header_data = line_iterator.next()
            except pb_escape.IterationError as e:
                self._file_error('Reading header: {}'.format(e))
            except StopIteration:
                header_data = None
            
            if header_data is None:
                # Build header.
                header_pb = pbt.PayloadInfo()
                header_pb.type = pb_type
                header_pb.pvname = self._pv_name
                header_pb.year = the_datetime.year
                
                # Write header. Note that since there was no header we are still at the start of the file.
                self._cur_file.write(pb_escape.escape_line(header_pb.SerializeToString()))
            
            else:
                # Parse header.
                header_pb = pbt.PayloadInfo()
                try:
                    header_pb.ParseFromString(header_data)
                except protobuf.message.DecodeError as e:
                    self._file_error('Failed to decode header: {}'.format(e))
                
                # Sanity checks.
                if header_pb.type != pb_type:
                    self._file_error('Type mispatch in header')
                if header_pb.pvname != self._pv_name:
                    self._file_error('PV name mispatch in header')
                if header_pb.year != the_datetime.year:
                    self._file_error('Year mispatch in header')
                
                # Iterate the file to the end, checking for problems.
                try:
                    for sample_data in line_iterator:
                        print('EXISTING SAMPLE')
                        
                        # Parse sample.
                        sample_pb = pb_class()
                        try:
                            sample_pb.ParseFromString(sample_data)
                        except protobuf.message.DecodeError as e:
                            self._file_error('Failed to decode sample: {}'.format(e))
                        
                        # Sanity check timestamp.
                        if (sample_pb.secondsintoyear, sample_pb.nano) > (into_year_sec, into_year_nsec):
                            self._file_error('Found sample newer than the sample we want to write')
                        
                except pb_escape.IterationError as e:
                    self._file_error('Reading samples: {}'.format(e))
        
        # Finally write the sample.
        self._cur_file.write(pb_escape.escape_line(sample_serialized))
    
    def _file_error(self, e):
        raise AppenderError('File {}: {}'.format(self._cur_path, e))
