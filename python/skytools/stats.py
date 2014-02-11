""" Statistics collection.

>>> class PrintSender(SkyLogHandler):
...     def output(self, txt):
...         print(txt)
>>> register_handler('print', PrintSender)
>>> #config_stats(10, 'print://')
>>> configure_handler('print://')#, args = {'interval': 60})
>>> configure_handler('tnetstr://localhost:23232?interval=10&qaz=wsx')
>>> configure_context(interval=10)
>>> ctx = get_collector('myjob')
>>> ctx.inc('count')
>>> ctx.inc('count')
>>> ctx.get_metric('count')
2
>>> ctx2 = ctx.get_collector('sub')
>>> ctx2.avg('duration', 0.5)
>>> import pprint
>>> pprint.pprint(_context.__dict__)
>>> for n,h in _context.handlers.items(): print n, pprint.pprint(h.__dict__)
>>> data1 = reset_stats()
>>> ctx.inc('count', 2)
>>> ctx2.avg('duration', 0.6)
>>> ctx2.set('cnt', Counter())
>>> ctx2.inc('cnt')
>>> ctx.get('count')
2
>>> ctx2.get('cnt')
1
>>> ctx.set('gauge', Gauge())
>>> ctx.inc('gauge')
>>> ctx.inc('gauge')
>>> ctx.set('midrange', GaugeMidRange(1))
>>> ctx.inc('midrange', 2)
>>> ctx.inc('midrange', 5)
>>> ctx.set('median', GaugeMedian(1))
>>> ctx.inc('median', 2)
>>> ctx.inc('median', 3)
>>> ctx.inc('median', 4)
>>> ctx.set('mode', GaugeMode(1))
>>> ctx.inc('mode', 1)
>>> ctx.inc('mode', 2)
>>> ctx.set('gmean', GaugeGMean(5))
>>> ctx.inc('gmean', 20)
>>> ctx.inc('gmean', 270)
>>> ctx.set('hmean', GaugeHMean(5))
>>> ctx.inc('hmean', 10)
>>> ctx.inc('hmean', 30)
>>> ctx.set('qmean', GaugeQMean(1))
>>> ctx.inc('qmean', 5)
>>> ctx.inc('qmean', 7)
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
>>> ctx.inc('tavg', 5)
>>> time.sleep(0.1)
>>> merge_stats(data1)
>>> process_stats(True)
{myjob.count: 4, myjob.sub.duration: 0.55}
"""
"""
>>> ctx.set('timer', Timer(0.2))
"""

import logging
import os.path
import time
import urlparse

import skytools
from stats_handlers import *
from stats_metrics import *


__all__ = ['get_collector', 'process_stats', 'merge_stats', 'register_handler',
           'config_stats', 'load_stats_conf']


class Context (object):
    def __init__(self):
        self.data = {}
        self.time = time.time()
        self.interval = 30
        self.handlers = {}

    def configure (self, **kwargs):
        for k,v in kwargs.items():
            if k in ['interval']:
                setattr(self, k, v)

_context = Context()

# _start = time.time()
# _state = {}

# _interval = 30
# _sender = None
_prefix = ''

_handlers = {} # registered handler classes (by scheme)


def load_stats_conf():
    fn = '/etc/stats.ini'
    if os.path.isfile(fn):
        cf = skytools.Config('stats', fn)
        ival = cf.getfloat('interval')
        backend = cf.get('backend')
        config_stats(ival, backend)
    else:
        config_stats(30, 'log')


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

    def get_metric(self, name):
        """ Get metric.
        """
        k = self.prefix + name
        try:
            m = self.ctx.data[k]
        except KeyError:
            m = None
        return m

    def get(self, name):
        """ Get a value (simple or eval'ed).
        """
        m = self.get_metric(name)
        try:
            value = m.eval()
        except AttributeError:
            value = m
        return value

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
    if not force and now - ctx.time < ctx.interval:
        return
    ctx.time = now
    data = reset_stats(ctx)
    for hname, handler in ctx.handlers.items():
        try:
            handler.process(data)
        except:
            logging.exception("Problem during stats processing [%s]", hname)


def merge_stats (data, context = None):
    """ Merge a stats dict with current one.
    """
    ctx = _get_context(context)
    for k, v in data.iteritems():
        try:
            ctx.data[k] += v
        except KeyError:
            ctx.data[k] = v
        except TypeError:
            ctx.data[k].merge(v)


def register_handler (scheme, cls):
    """ Register handler class for named scheme.
    """
    _handlers[scheme] = cls


def configure_context (context = None, **kwargs):
    ctx = _get_context(context)
    ctx.configure(**kwargs)


# def initialise_handler (backend, name = None, context = None):
def configure_handler (backend, name = None, context = None, **kwargs):
    """Set up stats backend.
    """
    ctx = _get_context(context)
    hid = name or backend
    hnd = None

    if backend.find(':') > 0:
        logging.error("BE: %r", backend)#XXX
        t = urlparse.urlparse(backend)
        logging.error("URL: %r", t)#XXX
        if t.scheme in _handlers:
            hnd = _handlers[t.scheme](t)
        else:
            logging.warning("Unknown stats handler: %s", t.scheme)
    elif backend in _handlers:
        hnd = _handlers[backend](None)
    else:
        logging.warning("Invalid stats handler: %r", backend)

    if not hnd:
        hnd = SkyLogHandler('')

    if name:
        hnd.name = name
    if kwargs:
        hnd.configure(**kwargs)
    ctx.handlers[hid] = hnd


register_handler('log', SkyLogHandler)
register_handler('tcp', SocketHandler)
register_handler('udp', DatagramHandler)
register_handler('tnetstr', UdpTNetStringsHandler)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
