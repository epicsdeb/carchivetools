from __future__ import print_function
import google.protobuf as protobuf
from carchive.pb import EPICSEvent_pb2 as pbt
from carchive.pb import escape as pb_escape

class EmptyFileError(Exception):
    pass

class VerificationError(Exception):
    pass

def verify_stream(stream, pb_type, pv_name, year, pb_class, upper_ts_bound):
    # Prepare line iterator.
    line_iterator = pb_escape.iter_lines(stream)
    
    # Check if we have a header.
    try:
        header_data = line_iterator.next()
    except pb_escape.IterationError as e:
        raise VerificationError('Reading header: {}'.format(e))
    except StopIteration:
        raise EmptyFileError()
    
    # Parse header.
    header_pb = pbt.PayloadInfo()
    try:
        header_pb.ParseFromString(header_data)
    except protobuf.message.DecodeError as e:
        raise VerificationError('Failed to decode header: {}'.format(e))
    
    # Sanity checks.
    if header_pb.type != pb_type:
        raise VerificationError('Type mispatch in header')
    if header_pb.pvname != pv_name:
        raise VerificationError('PV name mispatch in header')
    if header_pb.year != year:
        raise VerificationError('Year mispatch in header')
    
    # Iterate the file to the end, checking for problems.
    try:
        for sample_data in line_iterator:
            # Parse sample.
            sample_pb = pb_class()
            try:
                sample_pb.ParseFromString(sample_data)
            except protobuf.message.DecodeError as e:
                raise VerificationError('Failed to decode sample: {}'.format(e))
            
            # Sanity check timestamp.
            if upper_ts_bound is not None:
                if (sample_pb.secondsintoyear, sample_pb.nano) > upper_ts_bound:
                    raise VerificationError('Found newer sample')
            
    except pb_escape.IterationError as e:
        raise VerificationError('Reading samples: {}'.format(e))
