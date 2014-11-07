from __future__ import print_function
import os
import errno
from carchive.pb import filepath as pb_filepath
from carchive.pb import verify as pb_verify

class FindLastSampleError(Exception):
    pass

def find_last_sample_timestamp(pv_name, out_dir, gran, delimiters):
    # Get the directory path and the prefix of data files.
    dir_path, file_prefix = pb_filepath.get_dir_and_prefix(out_dir, delimiters, pv_name)
    
    # Collect the time suffixes of existing data files for this PV.
    try:
        time_suffixes = list(pb_filepath.filter_filenames(os.listdir(dir_path), file_prefix))
    except OSError as e:
        if e.errno == errno.ENOENT:
            time_suffixes = []
        else:
            raise
    
    # Split time suffixes into integer components, but keep the original suffixes around.
    time_suffixes = map(lambda x: {'suffix':x, 'ints':map(int, x.split('_'))}, time_suffixes)
    
    # Sanity check numer of components.
    num_comps = gran.suffix_count()
    for x in time_suffixes:
        if len(x['ints']) != num_comps:
            raise FindLastSampleError('Unexpected number of time suffix components: {}'.format(x['suffix']))
    
    # Sort suffixes.
    time_suffixes = sorted(time_suffixes, key=lambda x: x['ints'])
    
    # Have no suffixes? Then there are no samples at all.
    if len(time_suffixes) == 0:
        return None
    
    for suffix in reversed(time_suffixes):
        # Make the file path.
        file_path = pb_filepath.get_path_for_suffix(out_dir, delimiters, pv_name, suffix['suffix'])
        
        # Go through this file.
        with open(file_path, 'rb') as stream:
            results = pb_verify.verify_stream(stream, pv_name=pv_name)
        
        # If any samples were found in this file, the last timestamp in the
        # file is what we're looking for. Else continue looking into the previous file.
        if results['last_timestamp'] is not None:
            year = results['year']
            secondsintoyear, nano = results['last_timestamp']
            return (year, secondsintoyear, nano)
    
    # No samples found in any file.
    return None
