import re
import os
import platform

if platform.system() == 'Windows':
    pathName='{}@{}.pb'
    fileName = '{}@(.*).pb';
else:
    pathName='{}:{}.pb'
    fileName = '{}:(.*).pb';

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError:
        if not os.path.isdir(path):
            raise

def get_dir_and_prefix(out_dir, delimiters, pv_name):
    regexPattern = '|'.join(map(re.escape, delimiters))
    path_components = [out_dir] + re.split(regexPattern, pv_name)
    return (os.path.join(*path_components[:-1]), path_components[-1])

def get_path_for_suffix(out_dir, delimiters, pv_name, time_suffix):
    dir_path, file_prefix = get_dir_and_prefix(out_dir, delimiters, pv_name)
    make_sure_path_exists(dir_path)        
    return os.path.join(dir_path, pathName.format(file_prefix, time_suffix))

def filter_filenames(names, file_prefix):
    p = re.compile(fileName.format(file_prefix));
    for name in names:
        m = p.match(name)
        if m:
            yield m.group(1)
