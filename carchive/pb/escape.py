# Archiver PB files are split by lines. The first line is some header information.
# The remaining lines are samples. In these samples some characters are escaped.
# The escaping rules were obtained from LineEscaper.java.

NEWLINE_CHAR = '\x0A'

_ESCAPE_MAP = {
    '\x1B': '\x1B\x01',
    '\x0A': '\x1B\x02',
    '\x0D': '\x1B\x03',
}

def escape_line(data):
    return ''.join(_ESCAPE_MAP[c] if c in _ESCAPE_MAP else c for c in data)
