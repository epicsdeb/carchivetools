import re
import os

def get_dir_and_prefix(out_dir, delimiters, pv_name):
    regexPattern = '|'.join(map(re.escape, delimiters))
    path_components = [out_dir] + re.split(regexPattern, pv_name)
    return (os.path.join(*path_components[:-1]), path_components[-1])

def get_path_for_suffix(out_dir, delimiters, pv_name, time_suffix):
    dir_path, file_prefix = get_dir_and_prefix(out_dir, delimiters, pv_name)
    return os.path.join(dir_path, '{}:{}.pb'.format(file_prefix, time_suffix))

def filter_filenames(names, file_prefix):
    for name in names:
        m = re.match('\\A{}:(.*)\\.pb\\Z'.format(re.escape(file_prefix)), name)
        if m:
            yield m.group(1)
