# -*- coding: utf-8 -*-
"""
Copyright 2015 Brookhaven Science Assoc.
 as operator of Brookhaven National Lab.
"""
from __future__ import absolute_import

import logging
from six.moves import map
_log = logging.getLogger("carchive.h5data")

import h5py, numpy

__all__=['h5open','sevr2str']

_sevr={0:'',1:'MINOR',2:'MAJOR',3:'INVALID',
       3968:'Est_Repeat',3856:'Repeat',3904:'Disconnect',
       3872:'Archive_Off',3848:'Archive_Disable'
       }

def sevr2str(S):
    try:
        return _sevr[S]
    except KeyError:
        return str(S)

class H5PV(object):
    """The dataset(s) for a single PV

    Provides attributes: value, severity, status, and time
    """
    def __init__(self, name, G):
        self.name = name
        self.value = G['value']
        self.meta = G['meta']
        self.status = self.meta['status']
        self.severity = self.meta['severity']
        self.scalar = self.value.shape[1]==1

    def __len__(self):
        return self.meta.shape[0]

    @property
    def time(self):
        try:
            return self.__posix
        except AttributeError:
            self.__posix = P = self.meta['sec']+1e-9*self.meta['ns']
            return P

    def plotdata(self):
        """Return plot-able step data

        Returns a typle (time, value) where each is an array which has
        2*len(self)-1 points.  Each additional point is placed between
        a pair of input points in the input, and has the value of the
        preceding point with a time 1 ns before the time of the point
        which follows.

        Analogous to

        Input=[(T0,Y0),(T1,Y1)]
        Output[(T0,Y0),(T1-1e-9,Y0),(T1,Y1)]
        """
        if len(self)<=1:
            return self.time, self.value

        S = self.value.shape
        T = numpy.ndarray((2*S[0]-1,), dtype=self.time.dtype)
        V = numpy.ndarray((2*S[0]-1, S[1]), dtype=self.value.dtype)

        T[0::2] = self.time
        V[0::2] = self.value

        T[1::2] = self.time[1:]-1e-9
        V[1::2] = self.value[:-1,:]

        return T,V

class H5Data(object):
    """Access an HDF5 file containing data retrieved from PVs.

    >>> pvset = h5open('mydata.h5')
    >>> assert 'pvone' in pvset
    >>> pv1 = pvset['pvone']
    >>> allpvs = pvset.astuple()
    >>> assert pv1 in allpvs
    >>> val1 = pv1.value
    """
    def __init__(self, fname, mode='r'):
        name, _, path = fname.partition(':')

        self.__F=h5py.File(name, mode)
        self.__G=self.__F[path or '/']

        haspv=False
        for pv in self.__G:
            P = self.__G[pv]
            V, M = P.get('value',None), P.get('meta', None)
            if V and M and V.shape[0]==M.shape[0]:
                haspv=True
            elif not V and not M: # ignore unrelated
                continue
            else:
                _log.warn("%s/%s has incorrectly formatted data", fname, pv)

        if not haspv:
            raise ValueError("%s contains no data"%fname)

    def __len__(self):
        return len(self.__G)

    def __iter__(self):
        return iter(self.__G)

    def __contains__(self, key):
        return key in self.__G

    def __getitem__(self, key):
        return H5PV(key, self.__G[key])

    def astuple(self):
        """Return a tuple of H5PV instances.

        The order is establish by sorting the PV names.
        """
        pvs = list(self.__G)
        pvs.sort()
        return tuple(map(self.__getitem__, pvs))

h5open = H5Data
