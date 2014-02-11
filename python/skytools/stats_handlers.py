"""
Statistics collection -- Handlers (aka senders)
"""

import cPickle
import logging
import socket
import struct
import time

import skytools

# use fast implementation if available, otherwise fall back to reference one
try:
    import tnetstring as tnetstrings
    tnetstrings.parse = tnetstrings.pop
except ImportError:
    import skytools.tnetstrings as tnetstrings
    tnetstrings.dumps = tnetstrings.dump


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
        self.name = None
        self.args = skytools.db_urldecode (url.query if url else '')
        self.extra_attrs = {'type': lambda m: type(m).__name__} # XXX
        self.create_lock()

    def create_lock (self):
        """ Acquire a thread lock for serializing access to the underlying I/O. """
        if thread:
            self.lock = threading.RLock()
        else:
            self.lock = None

    def acquire (self):
        """ Acquire the I/O thread lock. """
        if self.lock:
            self.lock.acquire()

    def release (self):
        """ Release the I/O thread lock. """
        if self.lock:
            self.lock.release()

    def configure (self, **kwargs):
        for k,v in kwargs.items():
            if k in ['extra_attrs']:
                setattr(self, k, v)

    def enrich (self, metric):
        d = {}
        for k,v in self.extra_attrs.items():
            if callable(v):
                try:
                    d[k] = v(metric)
                except:
                    pass
            else:
                d[k] = v
        return d

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

    def __init__(self, url):
        super(SkyLogHandler, self).__init__(url)
        self.log = logging.getLogger()

    def emit (self, data):
        if len(data) == 0:
            return
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
        self.log.info(s)


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
        d.update(self.enrich(metric))
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
        try:
            d = metric.render_dict()
        except AttributeError:
            d = {'value': metric}
        d.update(self.enrich(metric))
        d.update(**kwargs)
        tnetstr = tnetstrings.dumps(d)
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

