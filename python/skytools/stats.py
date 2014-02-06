"""Stats collection.

>>> class PrintSender(SkyLogHandler):
...     def output(self, txt):
...         print(txt)
>>> register_handler('print', PrintSender)
>>> #config_stats(10, 'print://')
>>> configure_handler('print://')#, args = {'interval': 60})
>>> register_handler('tnetstr', UdpTNetStringsHandler)
>>> configure_handler('tnetstr://localhost:23232?interval=10&qaz=wsx')
>>> configure_context(interval=10)
>>> ctx = get_collector('myjob')
>>> ctx.inc('count')
>>> ctx.inc('count')
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
import math
import os.path
import socket
import struct
import time
import urlparse

import skytools

# use fast implementation if available, otherwise fall back to reference one
try:
    import tnetstring as tnetstrings
    tnetstrings.parse = tnetstrings.pop
except ImportError:
    #import skytools.tnetstrings as tnetstrings
    import tnetstrings as tnetstrings # XXX
    tnetstrings.dumps = tnetstrings.dump

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


#----------------------------------------------------------
# Metrics
#----------------------------------------------------------

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
    def render_dict (self):
        return {'value': self.eval()}

class GaugeMin (Gauge):
    def update (self, value):
        if self.value is None or value < self.value:
            self.value = value

class GaugeMax (Gauge):
    def update (self, value):
        if self.value is None or value > self.value:
            self.value = value

class GaugeMidRange (Gauge):
    """ mid-range / mid-extreme """
    def __init__(self, value = None):
        self.min = self.max = value
    def eval (self):
        return float (self.min + self.max) / 2
    def update (self, value):
        if self.min is None or value < self.min:
            self.min = value
        if self.max is None or value > self.max:
            self.max = value
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
        return self.data[0]


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
    if now - ctx.time < ctx.interval and not force:
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



#----------------------------------------------------------
#   Thread-related stuff
#----------------------------------------------------------

try:
    import thread
    import threading
except ImportError:
    thread = None

#
#_lock is used to serialize access to shared data structures in this module.
#This needs to be an RLock because fileConfig() creates and configures
#Handlers, and so might arbitrary user threads. Since Handler code updates the
#shared dictionary _handlers, it needs to acquire the lock. But if configuring,
#the lock would already have been acquired - so we need an RLock.
#The same argument applies to Loggers and Manager.loggerDict.
#
if thread:
    _lock = threading.RLock()
else:
    _lock = None

def _acquireLock():
    """
    Acquire the module-level lock for serializing access to shared data.

    This should be released with _releaseLock().
    """
    if _lock:
        _lock.acquire()

def _releaseLock():
    """
    Release the module-level lock acquired by calling _acquireLock().
    """
    if _lock:
        _lock.release()


#----------------------------------------------------------
#   Handlers
#----------------------------------------------------------


class Handler (object):
    """ Loosely based on logging.Handler """

    def __init__(self, url):
        """ url is urlparse() result """
        self._name = None
        self.create_lock()
        self.args = skytools.db_urldecode (url.query if url else '')

    def create_lock(self):
        """ Acquire a thread lock for serializing access to the underlying I/O. """
        if thread:
            self.lock = threading.RLock()
        else:
            self.lock = None

    def acquire(self):
        """ Acquire the I/O thread lock. """
        if self.lock:
            self.lock.acquire()

    def release(self):
        """ Release the I/O thread lock. """
        if self.lock:
            self.lock.release()

    def emit (self, data):
        """
        Do whatever it takes to actually process the stats data set.

        This version is intended to be implemented by subclasses.
        """
        raise NotImplementedError

    def process (self, data):
        """ Do whatever it takes to process the stats data set. """
        self.acquire()
        try:
            assert isinstance (data, dict)
            self.emit(data)
        finally:
            self.release()

    def close (self):
        """ Tidy up any resources used by the handler. """
        pass

    def handle_error (self, data):
        """ Handle errors which occur during an emit() call. """
        pass


class SkyLogHandler (Handler):
    """ Print stats to logfile (in classic skytools way). """

    def emit (self, data):
        try:
            buf = []
            for k in sorted(data.keys()):
                buf.append("%s: %s" % (k, data[k]))
            res = "{%s}" % ", ".join(buf)
            self.output(res)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handle_error(data)

    def output (self, s):
        logging.info(s)


class SocketHandler (Handler):
    """ Based on logging.SocketHandler """

    def __init__(self, url):
        """
        Initializes the handler with a specific host address and port.

        The attribute 'close_on_error' is set to 1 - which means that if
        a socket error occurs, the socket is silently closed and then
        reopened on the next logging call.
        """
        super(SocketHandler, self).__init__(url)
        logging.error("  %r %r", url.hostname, url.port)
        logging.error("  %r", self.args)
        self.host = url.hostname
        self.port = url.port
        self.sock = None
        self.close_on_error = 0
        self.retry_time = None
        #
        # Exponential backoff parameters.
        #
        self.retry_start = 1.0
        self.retry_max = 30.0
        self.retry_factor = 2.0

    def make_socket (self, timeout=1):
        """
        A factory method which allows subclasses to define the precise
        type of socket they want.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if hasattr(s, 'settimeout'):
            s.settimeout(timeout)
        s.connect((self.host, self.port))
        return s

    def create_socket (self):
        """
        Try to create a socket, using an exponential backoff with
        a max retry time. Thanks to Robert Olson for the original patch
        (SF #815911) which has been slightly refactored.
        """
        now = time.time()
        # Either retry_time is None, in which case this is the first time
        # back after a disconnect, or we've waited long enough.
        if self.retry_time is None:
            attempt = 1
        else:
            attempt = (now >= self.retry_time)
        if attempt:
            try:
                self.sock = self.make_socket()
                self.retry_time = None # next time, no delay before trying
            except socket.error:
                # Creation failed, so set the retry time and return.
                if self.retry_time is None:
                    self.retry_period = self.retry_start
                else:
                    self.retry_period *= self.retry_factor
                    if self.retry_period > self.retry_max:
                        self.retry_period = self.retry_max
                self.retry_time = now + self.retry_period

    def send (self, s):
        """
        Send a pickled string to the socket.

        This function allows for partial sends which can happen when the
        network is busy.
        """
        if self.sock is None:
            self.create_socket()
        # self.sock can be None either because we haven't reached the retry
        # time yet, or because we have reached the retry time and retried,
        # but are still unable to connect.
        if self.sock:
            try:
                if hasattr(self.sock, "sendall"):
                    self.sock.sendall(s)
                else:
                    sentsofar = 0
                    left = len(s)
                    while left > 0:
                        sent = self.sock.send(s[sentsofar:])
                        sentsofar = sentsofar + sent
                        left = left - sent
            except socket.error:
                self.sock.close()
                self.sock = None  # so we can call createSocket next time

    def make_pickle (self, metric, **kwargs):
        """
        Pickles the metric in binary format with a length prefix, and
        returns it ready for transmission across the socket.
        """
        try:
            d = metric.render_dict()
        except AttributeError:
            d = {'value': metric}
        d.update(**kwargs)
        s = cPickle.dumps(d, 1)
        slen = struct.pack(">L", len(s))
        return slen + s

    def handle_error (self, metric):
        """
        Handle an error during stats sending.
        """
        if self.close_on_error and self.sock:
            self.sock.close()
            self.sock = None  # try to reconnect next time
        else:
            super(SocketHandler, self).handle_error(metric)

    def emit (self, data):
        """
        Emit data set, metric by metric.

        Pickles the metrics and writes them to the socket in binary format.
        If there is an error with the socket, silently drop the packet.
        If there was a problem with the socket, re-establishes the socket.
        """
        try:
            for name, metric in data.iteritems():
                s = self.make_pickle (metric, name=name)
                self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handle_error(metric)

    def close (self):
        """
        Closes the socket.
        """
        self.acquire()
        try:
            if self.sock:
                self.sock.close()
                self.sock = None
        finally:
            self.release()
        super(SocketHandler, self).close()


class DatagramHandler (SocketHandler):
    """ Based on logging.DatagramHandler """

    def __init__(self, url):
        """
        Initializes the handler with a specific host address and port.
        """
        super(DatagramHandler, self).__init__(url)
        self.close_on_error = 0

    def make_socket (self):
        """
        The factory method of SocketHandler is here overridden to create
        a UDP socket (SOCK_DGRAM).
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return s

    def send (self, s):
        """
        Send a pickled string to a socket.

        This function no longer allows for partial sends which can happen
        when the network is busy - UDP does not guarantee delivery and
        can deliver packets out of sequence.
        """
        if self.sock is None:
            self.create_socket()
        self.sock.sendto(s, (self.host, self.port))


class UdpTNetStringsHandler (DatagramHandler):
    """ Sends stats in TNetStrings format over UDP. """

    _udp_reset = 0

    def make_pickle (self, metric, **kwargs):
        """ Create message in TNetStrings format.
        """
        msg = metric.render_dict()
        msg.update(**kwargs)
        tnetstr = tnetstrings.dumps(msg)
        return tnetstr

    def send (self, s):
        """ Cache socket for a moment, then recreate it.
        """
        now = time.time()
        if now - 1 > self._udp_reset:
            if self.sock:
                self.sock.close()
            self.sock = self.make_socket()
            self._udp_reset = now
        self.sock.sendto(s, (self.host, self.port))


def register_handler (scheme, cls):
    """ Register handler class for named scheme.
    """
    _handlers[scheme] = cls


def configure_context (context = None, **kwargs):
    ctx = _get_context(context)
    ctx.configure(**kwargs)


def configure_handler (backend, name = None, context = None):
    """Set up stats backend.
    """
    ctx = _get_context(context)
    hid = name or backend
    hnd = None

    if backend.find(':') > 0:
        logging.error("BE: %r", backend)
        t = urlparse.urlparse(backend)
        logging.error("URL: %r", t)
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

    ctx.handlers[hid] = hnd


register_handler('log', SkyLogHandler)


if __name__ == '__main__':
    import doctest
    doctest.testmod()
