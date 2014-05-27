
class HandledError(Exception):
    pass

class MultiProducerAdapter(object):
    def __init__(self, S=[]):
        self._set, self._paused = set(S), 0
    @property
    def paused(self):
        return self._paused>0
    @property
    def stopped(self):
        return self._paused==2
    def addProducer(self, prod):
        if self._paused:
            prod.pauseProducing()
        self._set.add(prod)
    def removeProducer(self, prod):
        self._set.remove(prod)
    def clear(self):
        self._set.clear()
    def pauseProducing(self):
        assert self._paused!=2
        if self._paused:
            return
        for P in self._set:
            P.pauseProducing()
        self._paused = 1
    def resumeProducing(self):
        assert self._paused!=2
        if not self._paused:
            return
        for P in self._set:
            P.resumeProducing()
        self._paused = 0
    def stopProducing(self):
        assert self._paused!=2
        for P in self._set:
            P.stopProducing()
        self._paused = 2
