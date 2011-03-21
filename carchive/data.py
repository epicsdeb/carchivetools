# -*- coding: utf-8 -*-
"""
Created on Sun Mar 20 10:25:53 2011

@author: mad
"""

import sys
from copy import copy
from date import makeTime, timeTuple

__all__=['DataHolder']

class DataHolder(object):
    _scalars=['name', 'type', 'how',
               'enums', 'precision', 'units',
               'upper_disp_limit','lower_disp_limit',
               'upper_ctrl_limit','lower_ctrl_limit',
               'upper_alarm_limit','lower_alarm_limit',
               'upper_warning_limit','lower_warning_limit',
               '__weakref__']
    _vectors=['value', 'status', 'severity', 'timestamp']
    __slots__=_scalars+_vectors

    def __unicode__(self):
        ret=''
        if hasattr(self, 'name'):
            ret+='%s '%self.name
        if hasattr(self, 'type'):
            ret+='(%s) '%self.type
        if not hasattr(self, 'value') or len(self.value)==0:
            return ret+' empty'
        if hasattr(self, 'timestamp') and len(self.timestamp)>0:
            S=makeTime(self.timestamp[0])
            E=makeTime(self.timestamp[-1])
            L=E-S
            return ret+'%d pts from %s for %s'%(len(self.timestamp),S,L)

    def __str__(self):
        return unicode(self)

    def __repr__(self):
        return u'DataHolder(%s)'%unicode(self)

    def __iadd__(self, other):
        """Concatinate with another DataHolder
        """
        if not isinstance(other, DataHolder):
            raise TypeError('Must be DataHolder')
        for chk in ['name', 'type', 'how']:
            if hasattr(self,chk) and hasattr(other,chk) \
                and getattr(self,chk)==getattr(other,chk):
                pass
            else:
                raise ValueError('Concatinated DataHolders must have the same %s'%chk)
        for cat in ['value', 'status', 'severity', 'timestamp']:
            R=getattr(self, cat)
            R+=getattr(other, cat)

    def pPrint(self, fmt='simple', fd=sys.stdout):
        for val, sts, sevr, ts in zip(self.value, self.status, self.severity, self.timestamp):
            print >>fd,makeTime(ts),' '.join(map(str,val)),
            if sts>0:
                print >>fd,sts,sevr
            else:
                print >>fd,''

    def pop(self, idx):
        r=DataHolder()
        for e in self._scalars:
            if hasattr(self, e):
                setattr(r, e, getattr(self,e))
        for v in self._vectors:
            if hasattr(self, v):
                setattr(r, v, [getattr(self,v).pop(idx)] )
        return r

def simpleMerge(dlist, start, end):
    """Take a list of DataHolder and attempt to concatinate non-overlapping
    ranges.
    Also truncates so that at most one sample before *start*.
    
    If successful, a single DataHolder is returned, otherwise a list representing
    "best effort" at concatinating is returned.
    """
    if len(dlist)==0:
        return dlist
    #start, end=timeTuple(start), timeTuple(end)

    dlist=copy(dlist)
    dlist.reverse()

    res=[]

    for r in dlist:
        # next range moving backward in time
        N=dlist.pop(0)
        res.append(N)
        if N.timestamp[0]<start:
            break

    dlist, res=res, []
    dlist.reverse()
    res.append(dlist.pop(0))

    for C in dlist:
        # moving forward
        P=res[-1]
        if P.timestamp[-1] < C.timestamp[0]:
            # non overlapping
            P+=C

        elif P.timestamp[-1] == C.timestamp[0]:
            # single duplicate sample
            C.pop(0)
            P+=C

        else:
            # non-trivial overlap
            res.append(C)

    if len(res)==0:
        return res[0]
    else:
        return res
