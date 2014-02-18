""" Statistics collection.

>>> import pprint
>>> class PrintSender(SkyLogHandler):
...     def output(self, txt):
...         print(txt)
>>> register_handler('print', PrintSender)
>>> #c = load_stats_config( extra_attrs = {'type': "NONE"} )
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
>>> #time.sleep(0.2)
>>> #ctx.inc('tavg', 5)
>>> #time.sleep(0.1)
>>> merge_stats(data1)
>>> process_stats(True)
{myjob.count: 4, myjob.gauge: 1, myjob.gmean: 30.0, myjob.hmean: 9.0, myjob.median: 2.5, myjob.midrange: 3.0, \
myjob.mode: 1, myjob.qmean: 5.0, myjob.sub.cnt: 1, myjob.sub.duration: 0.55, myjob.tavg: 2.0, myjob.wamean: 2.6, \
myjob.wgmean: -1.0324790671, myjob.whmean: 30.7692307692}
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


def_config_files = ['skystats.ini', 'stats.ini',
                    '~/.skystats.ini', '~/.stats.ini',
                    '/etc/skystats.ini', '/etc/stats.ini']


class Context (object):
    def __init__(self):
        self.data = {}
        self.time = time.time()
        self.interval = 30
        self.handlers = {}
        self.log = logging.getLogger()

    def configure (self, **kwargs):
        for k,v in kwargs.items():
            if k in ['interval', 'log']:
                setattr(self, k, v)

    def merge_stats (self, data):
        """ Merge a stats dict with current one.
        """
        for k, v in data.iteritems():
            try:
                self.data[k] += v
            except KeyError:
                self.data[k] = v
            except TypeError:
                self.data[k].merge(v)

    def process_stats (self, force = False):
        """ Check if interval is over, then send stats.
        """
        now = time.time()
        if not force and now - self.time < self.interval:
            return
        self.time = now
        data = self.reset_stats()
        for hname, handler in self.handlers.items():
            try:
                handler.process(data)
            except:
                self.log.exception ("Problem during stats processing [%s]", hname)

    def reset_stats (self):
        """ Return current data, resetting it.
        """
        cur_data = self.data
        self.data = {}
        return cur_data


_context = Context()

_handlers = {} # registered handler classes (by scheme)


class StatsContext (object):
    """ Stats collector -- user level API to basic features and namespaces.
    """
    __slots__ = ['ctx', 'prefix', 'delim']

    def __init__(self, name = None, delim = '.', context = None):
        self.ctx = context or _context
        if name:
            self.prefix = name + delim
        else:
            self.prefix = ''
        self.delim = delim

    def get_collector (self, name):
        """ New collector context under this one.
        """
        return StatsContext (self.prefix + name, self.delim, self.ctx)

    def get_handler (self, name):
        """ Return existing handler.
        """
        return self.ctx.handlers.get(name)

    def get_metric (self, name):
        """ Return existing metric.
        """
        k = self.prefix + name
        return self.ctx.data.get(k)

    def get (self, name):
        """ Get a value (simple or eval'ed).
        """
        m = self.get_metric(name)
        try:
            value = m.eval()
        except AttributeError:
            value = m
        return value

    def set (self, name, value):
        """ Set to a value.
        """
        k = self.prefix + name
        self.ctx.data[k] = value

    def inc (self, name, value = 1):
        """ Add a value to be summed.
        """
        k = self.prefix + name
        try:
            self.ctx.data[k] += value
        except KeyError:
            self.ctx.data[k] = value
        except TypeError:
            self.ctx.data[k].update(value)

    def avg (self, name, value):
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
    """ Start up new namespace.
    """
    return StatsContext(name)


def reset_stats (context = None):
    """ Return current data, resetting it.
    """
    ctx = _get_context(context)
    return ctx.reset_stats()


def process_stats (force = False, context = None):
    """ Check if interval is over, then send stats.
    """
    ctx = _get_context(context)
    ctx.process_stats(force=force)


def merge_stats (data, context = None):
    """ Merge a stats dict with current one.
    """
    ctx = _get_context(context)
    ctx.merge_stats(data)


def register_handler (scheme, cls):
    """ Register handler class for named scheme.
    """
    _handlers[scheme] = cls


def configure_context (context = None, **kwargs):
    """ Configure per-context parameters.
    """
    ctx = _get_context(context)
    ctx.configure(**kwargs)


def configure_handler (backend, name = None, context = None, **kwargs):
    """ Set up stats backend.
    """
    ctx = _get_context(context)
    hid = name or backend

    # initialise new handler instance
    if backend.find(':') > 0:
        t = urlparse.urlparse(backend)
        if t.scheme in _handlers:
            hnd = _handlers[t.scheme](t)
        else:
            raise Exception ("Unknown stats handler: %s" % t.scheme)
    elif backend in _handlers:
        hnd = _handlers[backend](None)
    else:
        raise Exception ("Invalid stats handler: %r" % backend)

    # convenient configuration
    if hasattr(hnd, 'log'):
        hnd.configure(log = ctx.log)

    # additional configuration
    if kwargs:
        hnd.configure(**kwargs)
    if name:
        hnd.name = name

    ctx.handlers[hid] = hnd


def load_stats_config (filename = None, section_name = None, context_name = None,
                       context_params = {}, handler_params={}, extra_attrs = {}):
    """
    Load statistics configuration from a file, apply user defined overrides.
    """

    def read_params (cf, plist_name, pitem_name):
        plist = cf.getdict (plist_name, {})
        params = {}
        for pname, ptype in plist.items():
            method = getattr(cf, "get" + ptype, cf.get)
            params[pname] = method (pitem_name % pname)
        return params

    # locate config file

    if filename:
        if isinstance(filename, list):
            flist = filename
        else:
            flist = [filename]
    else:
        flist = def_config_files

    for _fn in flist:
        fn = os.path.expanduser(_fn)
        if os.path.isfile(fn):
            break
    else:
        fn = None

    if not fn:
        raise Exception ("stats config file not found: %s" % filename)

    # configure context

    if context_name is None:
        context = _context
    else:
        context = Context()

    mcf = skytools.Config (section_name or "default", fn, ignore_defs = True)

    cfg_ctx_params = read_params(mcf, "params", "param_%s")
    context.configure(**cfg_ctx_params)
    context.configure(**context_params)

    # configure handlers

    handlers = mcf.getlist("handlers", [])
    hsprefix = mcf.get("handler_section_prefix", "handler_")

    for hname in handlers:
        # read per-handler config
        scf = mcf.clone (hsprefix + hname)
        url = scf.get("backend", "")
        name = scf.get("name", "")
        elist = scf.getlist("extra_attrs", [])
        extra = dict((a,v) for a,v in extra_attrs.items() if a in elist)
        hnd_params = read_params(scf, "params", "param_%s")
        hnd_params.update(handler_params)
        # create new handler
        configure_handler(url or hname, name = name, context = context, extra_attrs = extra, **hnd_params)

    return context


register_handler('log', SkyLogHandler)
register_handler('tcp', SocketHandler)
register_handler('udp', DatagramHandler)
register_handler('tnetstr', UdpTNetStringsHandler)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
