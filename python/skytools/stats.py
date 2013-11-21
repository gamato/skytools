"""Stats collection.

>>> class PrintSender(LogSender):
...     def output(self, txt):
...         print(txt)
>>> register_sender('print', PrintSender)
>>> config_stats(10, 'print://')
>>> ctx = get_collector('myjob')
>>> ctx.inc('count')
>>> ctx.inc('count')
>>> ctx2 = ctx.get_collector('sub')
>>> ctx2.avg('duration', 0.5)
>>> data1 = reset_stats()
>>> ctx.inc('count', 2)
>>> ctx2.avg('duration', 0.6)
>>> ctx2.set('cnt', Counter())
>>> ctx2.inc('cnt')
>>> ctx.set('gmean', GaugeGMean(5))
>>> ctx.inc('gmean', 20)
>>> ctx.inc('gmean', 270)
>>> ctx.set('hmean', GaugeHMean(5))
>>> ctx.inc('hmean', 10)
>>> ctx.inc('hmean', 30)
>>> ctx.set('wamean', GaugeWAMean())
>>> ctx.inc('wamean', (1, 5))
>>> ctx.inc('wamean', (2, 20))
>>> ctx.inc('wamean', (3, 50))
>>> ctx.set('wgmean', GaugeWGMean())
>>> ctx.inc('wgmean', (2, 3))
>>> ctx.inc('wgmean', (3, 2))
>>> ctx.inc('wgmean', (4, 1))
>>> ctx.set('whmean', GaugeWHMean())
>>> ctx.inc('whmean', (40, 70))
>>> ctx.inc('whmean', (25, 25))
>>> ctx.inc('whmean', (10, 5))
>>> ctx.set('tavg', GaugeTimedAvg(2))
>>> time.sleep(0.2)
>>> ctx.inc('tavg', 3)
>>> time.sleep(0.1)
>>> merge_stats(data1)
>>> process_stats(True)
{myjob.count: 4, myjob.sub.duration: 0.55}
"""
"""
>>> ctx.set('timer', Timer(0.2))
"""

import logging
import math
import os.path
import time
import urlparse

__all__ = ['get_collector', 'process_stats', 'merge_stats', 'register_sender',
           'config_stats', 'load_stats_conf']


class Context (object):
    def __init__(self):
        self.data = {}
        self.time = time.time()

_context = Context()

_start = time.time()
_state = {}

_interval = 30
_sender = None
_prefix = ''

_log_handlers = {}

def register_sender(scheme, sender_class):
    """Register sender class for named schema.
    """
    _log_handlers[scheme] = sender_class

class StatSender(object):
    """Base class for senders.
    """
    def __init__(self, url):
        """url is urlparse() result.
        """
        pass

    def send(self, data):
        """Send stats out.
        """
        pass

class LogSender(StatSender):
    """Print stats to logfile.
    """
    def send(self, data):
        buf = []
        keys = data.keys()
        keys.sort()
        for k in keys:
            v = data[k]
            if isinstance(v, list):
                val = v[0] / v[1]
            else:
                val = v
            buf.append("%s: %s" % (k, val))
        res = "{%s}" % ", ".join(buf)
        self.output(res)

    def output(self, txt):
        logging.info(txt)

register_sender('log', LogSender)

def config_stats(interval, backend):
    """Set up stats backend.
    """
    global _interval, _sender
    _interval = interval
    if backend.find(':') > 0:
        t = urlparse.urlparse(backend)
        if t.scheme in _log_handlers:
            _sender = _log_handlers[t.scheme](t)
        else:
            logging.warning("Unknown stats sender: %s", t.scheme)
    elif backend in _log_handlers:
        _sender = _log_handlers[backend](None)
    else:
        logging.warning("Invalid stats sender: %r", backend)

    if not _sender:
        _sender = LogSender(None)

def load_stats_conf():
    fn = '/etc/stats.ini'
    if os.path.isfile(fn):
        cf = skytools.Config('stats', fn)
        ival = cf.getfloat('interval')
        backend = cf.get('backend')
        config_stats(ival, backend)
    else:
        config_stats(30, 'log')

#--------------------------------------
# Metrics
#--------------------------------------

class Metric (object):
    #def recalc (self):
    #    raise NotImplementedError
    def render (self):
        pass
    def render_text (self):
        raise NotImplementedError
    def render_json (self):
        raise NotImplementedError
    def render_netstr (self):
        raise NotImplementedError
    def reset (self):
        raise NotImplementedError

class Counter (Metric):
    def __init__(self, value = 0):
        self.value = value
    def __str__(self):
        return str(self.value)
    #def recalc (self):
    #    pass
    def update (self, delta):
        self.value += delta

class Timer (Metric):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return str(self.value)
    def update (self, delta):
        self.value += delta

class Gauge (Metric):
    def __init__(self, value = None):
        self.value = value
    def __str__(self):
        return str(self.eval())
    def eval (self):
        return self.value
    def update (self, value):
        self.value = value
    def render_text (self):
        return self.__str__()

class GaugeAMean (Gauge):
    """ arithmetic mean """
    def __init__(self, value = None):
        if value is None:
            self.count = 0
            self._temp = 0
        else:
            self.count = 1
            self._temp = value
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
        if value is None:
            self.count = 0
            self._temp = 1
        else:
            assert value > 0
            self.count = 1
            self._temp = value
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
        if value is None:
            self.count = 0
            self._temp = 0
        else:
            assert value > 0
            self.count = 1
            self._temp = 1./value
    def update (self, value):
        assert value > 0
        self.count += 1
        self._temp += 1./value
    def merge (self, that):
        self.count += that.count
        self._temp += that._temp
    def eval (self):
        return self.count / self._temp

class GaugeWAMean (Gauge):
    """ weighted arithmetic mean """
    def __init__(self, data = None):
        # data = (value, weight)
        self.data = data or (0.0, 0.0) # no effect
        assert self.data[1] >= 0
    def eval (self):
        return self.data[0]
    def update (self, data):
        assert data[1] >= 0
        weights = self.data[1] + data[1]
        self.data = ((self.data[0] * self.data[1] + data[0] * data[1]) / weights, weights)

class GaugeWGMean (Gauge):
    """ weighted geometric mean """
    def __init__(self, data = None):
        # data = (value, weight)
        self.data = data or (1.0, 0.0) # no effect
        assert self.data[0] > 0 and self.data[1] >= 0
    def eval (self):
        return self.data[0]
    def update (self, data):
        assert data[0] > 0 and data[1] >= 0
        weights = self.data[1] + data[1]
        self.data = ((math.log(self.data[0]) * self.data[1] + math.log(data[0]) * data[1]) / weights, weights)

class GaugeWHMean (Gauge):
    """ weighted harmonic mean """
    def __init__(self, data = None):
        # data = (value, weight)
        self._sum_v = self._sum_w = 0
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
        return self.data[0]

# 2 2 2 3 3
# 2 2 2 2.25 2.4

class GaugeMin (Gauge):
    def __init__(self, value):
        super(GaugeMin, self).__init__(value)
        self.min = value
    def update(self, value):
        if value < self.min:
            self.min = value


class StatsContext (object):
    """Position in namespace .
    """
    __slots__ = ['ctx', 'prefix', 'delim']

    def __init__(self, name = None, delim = '.', context = None):
        self.ctx = context or _context
        if name:
            self.prefix = name + delim
        else:
            self.prefix = ''
        self.delim = delim

    def get_collector(self, name):
        """ New collector context under this one.
        """
        return StatsContext(self.prefix + name, self.delim, self.ctx)

    def set(self, name, value):
        """ Set to a value.
        """
        k = self.prefix + name
        self.ctx.data[k] = value

    def inc(self, name, value = 1):
        """ Add a value to be summed.
        """
        k = self.prefix + name
        try:
            self.ctx.data[k] += value
        except KeyError:
            self.ctx.data[k] = value
        except TypeError:
            self.ctx.data[k].update(value)

    def avg(self, name, value):
        """ Add a value to be averaged.
        """
        k = self.prefix + name
        try:
            self.ctx.data[k].update(value)
        except KeyError:
            self.ctx.data[k] = GaugeAvg(value)


def _get_context (ctx):
    if ctx is None:
        return _context
    elif isinstance(ctx, Context):
        return ctx
    elif isinstance(ctx, StatsContext):
        return ctx.ctx
    raise ValueError ("Wrong context passed")


def get_collector (name = None):
    """Start up new namespace.
    """
    if name and _prefix: # XXX: what for?
        return StatsContext(_prefix + '.' + name)
    return StatsContext(name)


def reset_stats (context = None):
    """ Return current data, resetting it.
    """
    ctx = _get_context(context)
    cur_data = ctx.data
    ctx.data = {}
    return cur_data


def process_stats (force = False, context = None):
    """ Check if interval is over, then send stats.
    """
    ctx = _get_context(context)
    now = time.time()
    if now - ctx.time < _interval and not force:
        return
    ctx.time = now

    try:
        _sender.send(reset_stats())
    except:
        logging.exception("Problem during stats send")


def merge_stats (data, context = None):
    """ Merge a stats dict with current one.
    """
    ctx = _get_context(context)
    for k, v in data.items():
        try:
            ctx.data[k] += v
        except KeyError:
            ctx.data[k] = v
        except TypeError:
            ctx.data[k].merge(v)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
