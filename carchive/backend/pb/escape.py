# Archiver PB files are split by lines. The first line is some header information.
# The remaining lines are samples. In these samples some characters are escaped.
# The escaping rules were obtained from LineEscaper.java.
import re
try:
    from carchive.backend.pbdecode import unescape
except ImportError:
    cppunescape=None


NEWLINE_CHAR = '\x0A'

_ESCAPE_MAP = {
    '\x1B': '\x1B\x01',
    '\x0A': '\x1B\x02',
    '\x0D': '\x1B\x03',
}

_UNESCAPE_MAP = {
    '\x01': '\x1B',
    '\x02': '\x0A',
    '\x03': '\x0D',
}

R=re.compile(r'[\x1b\x0a\x0d]')
def X(M):
    return _ESCAPE_MAP[M.group(0)]
    
def escape_line(data):
    #return ''.join(_ESCAPE_MAP[c] if c in _ESCAPE_MAP else c for c in data) + NEWLINE_CHAR
    return R.sub(X, data) + NEWLINE_CHAR

class UnescapeError(Exception):
    pass

def unescape_data(data):
    if cppunescape:
        return unescape(data)
    else:
        res = ''
        i = 0
        l = len(data)
        while i < l:
            c = data[i]
            i += 1
            if c == '\x1B':
                if not i < l:
                    raise UnescapeError('Short escape sequence')
                d = data[i]
                i += 1
                if d not in _UNESCAPE_MAP:
                    raise UnescapeError('Invalid escape sequence')
                c = _UNESCAPE_MAP[d]
            res += c
        return res

class IterationError(Exception):
    pass

def iter_lines(stream):
    buf = ''
    while True:
        newbuf = stream.read(4096)
        if not newbuf:
            if len(buf) > 0:
                raise IterationError('Missing line terminator at end of file')
            break
        parts = newbuf.split(NEWLINE_CHAR)
        buf += parts[0]
        for i in range(1, len(parts)):
            try:
                unescaped = unescape_data(buf)
            except UnescapeError as e:
                raise IterationError(e)
            yield unescaped
            buf = parts[i]
