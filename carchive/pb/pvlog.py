from __future__ import print_function

class SeverityError(object):
    TEXT = 'ERROR'

class SeverityWarning(object):
    TEXT = 'WARNING'

class SeverityInfo(object):
    TEXT = 'INFO'

class PvLog(object):
    def __init__(self, pv_name):
        self._pv_name = pv_name
        self._archived_count = 0
        self._initial_ignored_count = 0
        self._messages = []
    
    def archived_sample(self):
        self._archived_count += 1
    
    def ignored_initial_sample(self):
        self._initial_ignored_count += 1
    
    def message(self, text, severity):
        msg = {'text': text, 'severity': severity}
        self._messages.append(msg)
        print('{}'.format(self._format_message(msg)))
    
    def error(self, text):
        self.message(text, severity=SeverityError)
    
    def warning(self, text):
        self.message(text, severity=SeverityWarning)
    
    def info(self, text):
        self.message(text, severity=SeverityInfo)
    
    def has_errors(self):
        return any((msg['severity'] is SeverityError) for msg in self._messages)
    
    def build_report(self):
        error_count = sum((msg['severity'] is SeverityError) for msg in self._messages)
        warning_count = sum((msg['severity'] is SeverityWarning) for msg in self._messages)
        header_row = '{}: Archived={}, InitialIgnored={}, Errors={}, Warnings={}\n'.format(self._pv_name, self._archived_count, self._initial_ignored_count, error_count, warning_count)
        message_rows = ''.join('  {}\n'.format(self._format_message(msg)) for msg in self._messages if msg['severity'] is not SeverityInfo)
        return header_row + message_rows
    
    def _format_message(self, msg):
        return '{}: {}'.format(msg['severity'].TEXT, msg['text'])
