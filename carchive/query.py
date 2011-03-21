# -*- coding: utf-8 -*-
"""
Archiver query builder

@author: mad
"""

import datetime

__all__ = ['ArchiveQuery']

class ArchiveQuery(object):
    """ArchiveQuery(archiver)
    
    Helper holds arguments used to query the archiver.
    
    If unspecified *how* is 'raw', archs is '*', count is 1,
    and end is the time when execute() is invoked.
    
    >>> q=ArchiveQuery(archiver)
    >>> q.names='SR-*'
    >>> q.count=100
    >>> q.start='-1 hour'
    >>> data1=q.execute()
    >>> q.start='-30 m'
    >>> data2=q.execute()
    """
    
    names=None
    patterns=None
    how=None
    archs=None
    start=None
    end=None
    count=None

    def __init__(self, archiver):
        self.__serv=archiver
        self.clear()
    
    def clear(self):
        self.names=None
        self.patterns=False
        self.how='raw'
        self.archs='*'
        self.start=None
        self.end=None
        self.count=1

    def set(self, **kws):
        """Set attributes
        
        >>> a=ArchiveQuery(archiver)
        >>> b=a.set(names='test*', start='-1 m').set(count=10)
        >>> a is b
        True
        """
        for k, v in kws.iteritems():
            setattr(self, k, v)
        return self

    def execute(self):
        """Run requested query
        Returns data and modifies internal state.
        
        *names* and *archs* are expanded and *patterns* is made False.
        If unset, *end* is set to the system time when the query was
        executed.
        """
        if isinstance(self.archs, str):
            self.archs=self.__serv.archs(pattern=self.archs)

        if isinstance(self.names, str):
            self.names=[self.names]

        if self.patterns:
            res=set()
            for n in self.names:
                # TODO: reduce self.archs to only those archives which
                #       actually had interesting names
                res|=set(self.__serv.search(n, archs=self.archs).keys())
            self.names=list(res)
        self.patterns=False

        if self.end is None:
            self.end=datetime.datetime.now()

        return self.__serv.get(self.names, self.start, self.end,
                               count=self.count, how=self.how,
                               archs=self.archs)
