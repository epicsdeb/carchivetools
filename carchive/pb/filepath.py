import re
import os
import platform

def get_dir_and_prefix(out_dir, delimiters, pv_name):
    regexPattern = '|'.join(map(re.escape, delimiters))
    path_components = [out_dir] + re.split(regexPattern, pv_name)
    return (os.path.join(*path_components[:-1]), path_components[-1])

def get_path_for_suffix(out_dir, delimiters, pv_name, time_suffix):
    dir_path, file_prefix = get_dir_and_prefix(out_dir, delimiters, pv_name)
    name = '{}:{}.pb'
    if platform.system() == 'Windows':
        name = '{}@{}.pb'
    return os.path.join(dir_path, name.format(file_prefix, time_suffix))

def filter_filenames(names, file_prefix):
    reg = '\\A{}:(.*)\\.pb\\Z';
    if platform.system() == 'Windows':
        reg = '\\A{}@(.*)\\.pb\\Z'
    for name in names:
        m = re.match(reg.format(re.escape(file_prefix)), name)
        if m:
            yield m.group(1)
