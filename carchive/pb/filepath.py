import re
import os

def get_path_for_suffix(out_dir, delimiters, pv_name, time_suffix):
    regexPattern = '|'.join(map(re.escape, delimiters))
    path_components = [out_dir] + re.split(regexPattern, pv_name)
    path_components[-1] += ':{}.pb'.format(time_suffix)
    return os.path.join(*path_components)
