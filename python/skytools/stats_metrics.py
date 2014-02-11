"""
Statistics collection -- Metrics (counters, gauges, timers)
"""

import math
import time

class Metric (object):
    def update (self, val):
        raise NotImplementedError
    def merge (self, that):
        raise NotImplementedError
    def eval (self):
        raise NotImplementedError
    def render_text (self):
        raise NotImplementedError
    def render_dict (self):
        raise NotImplementedError
    def render_json (self):
        raise NotImplementedError
    def render_netstr (self):
        raise NotImplementedError
    def reset (self):
        raise NotImplementedError
    def __str__(self):
        return self.render_text()

#----------------------------------------------------------
# Counters
#----------------------------------------------------------

class Counter (Metric):
    def __init__(self, value = 0):
        self.value = value
    def update (self, delta):
        self.value += delta
    def merge (self, that):
        self.update (that.value)
    def eval (self):
        return self.value
    def render_text (self):
        return str(self.eval())
    def render_dict (self):
        return {'value': self.eval()}

#----------------------------------------------------------
# Gauges
#----------------------------------------------------------

class Gauge (Metric):
    def __init__(self, value = None):
        self.value = value
    def update (self, value):
        self.value = value
    def eval (self):
        return self.value
    def render_text (self):
        return str(self.eval())
    def render_dict (self):
        return {'value': self.eval()}

class GaugeMin (Gauge):
    def update (self, value):
        if self.value is None or value < self.value:
            self.value = value
    def merge (self, that):
        self.update (that.value)

class GaugeMax (Gauge):
    def update (self, value):
        if self.value is None or value > self.value:
            self.value = value
    def merge (self, that):
        self.update (that.value)

class GaugeMidRange (Gauge):
    """ mid-range / mid-extreme """
    def __init__(self, value = None):
        self.min = self.max = value
    def update (self, value):
        if self.min is None or value < self.min:
            self.min = value
        if self.max is None or value > self.max:
            self.max = value
    def merge (self, that):
        if self.min is None or that.min < self.min:
            self.min = that.min
        if self.max is None or that.max > self.max:
            self.max = that.max
    def eval (self):
        return float (self.min + self.max) / 2
    def render_dict (self):
        d = super(GaugeMidRange, self).render_dict()
        d.update (min = self.min, max = self.max)
        return d

class GaugeMedian (Gauge):
    def __init__(self, value = None):
        self._data = []
        if value is not None:
            self.update(value)
    def update (self, value):
        self._data.append(value)
    def merge (self, that):
        self._data.extend(that._data)
    def eval (self):
        self._data.sort()
        n = len(self._data)
        m = n >> 1
        if n % 2:
            return self._data[m]
        else:
            return float (self._data[m-1] + self._data[m]) / 2

class GaugeMode (Gauge):
    def __init__(self, value = None):
        self._data = {}
        if value is not None:
            self.update(value)
    def update (self, value):
        try:
            self._data[value] += 1
        except KeyError:
            self._data[value] = 1
    def merge (self, that):
        for k,v in that._data.iteritems():
            try:
                self._data[k] += v
            except KeyError:
                self._data[k] = v
    def eval (self):
        m = max(self._data.iteritems(), key = lambda t: t[1])
        return m[0]

class GaugeAMean (Gauge):
    """ arithmetic mean """
    def __init__(self, value = None):
        self.count = 0
        self._temp = 0
        if value is not None:
            self.update(value)
    def update (self, value):
        self.count += 1
        self._temp += value
    def merge (self, that):
        self.count += that.count
        self._temp += that._temp
    def eval (self):
        return self._temp / float(self.count)
    #def reset (self):
    #    self.count = self.value = 0

GaugeAvg = GaugeAMean

class GaugeGMean (Gauge):
    """ geometric mean """
    def __init__(self, value = None):
        self.count = 0
        self._temp = 1
        if value is not None:
            self.update(value)
    def update (self, value):
        assert value > 0
        self.count += 1
        self._temp *= value
    def merge (self, that):
        self.count += that.count
        self._temp *= that._temp
    def eval (self):
        return math.pow (self._temp, 1./self.count)
    #def reset (self):
    #    self.count = self.value = 0

class GaugeHMean (Gauge):
    """ harmonic mean """
    def __init__(self, value = None):
        self.count = 0
        self._temp = 0
        if value is not None:
            self.update(value)
    def update (self, value):
        assert value > 0
        self.count += 1
        self._temp += 1./value
    def merge (self, that):
        self.count += that.count
        self._temp += that._temp
    def eval (self):
        return self.count / self._temp

class GaugeQMean (Gauge):
    """ quadratic mean / root mean square """
    def __init__(self, value = None):
        self.count = 0
        self._temp = 0
        if value is not None:
            self.update(value)
    def update (self, value):
        self.count += 1
        self._temp += value**2
    def merge (self, that):
        self.count += that.count
        self._temp += that._temp
    def eval (self):
        return math.sqrt (self._temp / float(self.count))

GaugeRMS = GaugeQMean

class GaugeWAMean (Gauge):
    """ weighted arithmetic mean """
    def __init__(self, data = None):
        # data = (value, weight)
        self.data = data or (0.0, 0.0) # no effect
        assert self.data[1] >= 0
    def update (self, data):
        assert data[1] >= 0
        weights = self.data[1] + data[1]
        self.data = ((self.data[0] * self.data[1] + data[0] * data[1]) / weights, weights)
    def eval (self):
        return self.data[0]

class GaugeWGMean (Gauge):
    """ weighted geometric mean """
    def __init__(self, data = None):
        # data = (value, weight)
        self.data = data or (1.0, 0.0) # no effect
        assert self.data[0] > 0 and self.data[1] >= 0
    def update (self, data):
        assert data[0] > 0 and data[1] >= 0
        weights = self.data[1] + data[1]
        self.data = ((math.log(self.data[0]) * self.data[1] + math.log(data[0]) * data[1]) / weights, weights)
    def eval (self):
        return self.data[0]

class GaugeWHMean (Gauge):
    """ weighted harmonic mean """
    def __init__(self, data = None):
        # data = (value, weight)
        self._sum_v = 0
        self._sum_w = 0
        if data is not None:
            self.update(data)
    def update (self, data):
        assert data[0] > 0 and data[1] >= 0
        self._sum_v += data[1] / float(data[0])
        self._sum_w += data[1]
    def eval (self):
        return self._sum_w / self._sum_v

class GaugeTimedAvg (GaugeWAMean):
    """ GaugeWAMean where weight is time passed """
    def __init__(self, value):
        super(GaugeTimedAvg, self).__init__()
        self._time = time.time()
        self._cval = value
    def update (self, value):
        now = time.time()
        data = (self._cval, now - self._time)
        super(GaugeTimedAvg, self).update(data)
        self._cval = value
        self._time = now
    def eval (self):
        self.update(self._cval)
        return super(GaugeTimedAvg, self).eval()

#----------------------------------------------------------
# Timers
#----------------------------------------------------------

class Timer (Metric):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return str(self.value)
    def update (self, delta):
        self.value += delta

