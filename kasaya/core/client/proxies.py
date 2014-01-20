#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import messages
from kasaya.conf import settings
from kasaya.core.protocol.comm import send_and_receive_response, send_and_receive
from kasaya.core import exceptions
from kasaya.core.lib import LOG
from kasaya.core.lib.syncclient import KasayaLocalClient
from kasaya.core.client.worker_finder import WorkerFinder


# on python 2 we need to fix some strings to unicode
# this is required to work with python 3 workers called with python 2 clients
import sys
_namefix = sys.version_info<(3,0)
del sys

__all__ = ("SyncProxy", "AsyncProxy", "ControlProxy", "TransactionProxy")



class GenericProxy(object):
    """
    Catch calls separated by dots:
    module.submodule.function(...)
    """

    def __init__(self):
        self._names = []
        self._method = None
        self._context = None

    def initialize(self, method, context):
        self._names = method
        self._context = context
        self._addr = self.find_worker(method)

    def __getattr__(self, itemname):
        self._names.append(itemname)
        return self

    def _find_worker(self, service_name, method=None):
        """
        Ask kasaya daemon where is service
        """
        wfinder = WorkerFinder()
        return wfinder.find_worker(service_name)

    @staticmethod
    def _namefixer(msg):
        global _namefix
        if _namefix:
            try:
                s = msg['service']
                if type(s)!=unicode:
                    msg['service'] = unicode(s,'ascii')
            except KeyError:
                pass
            try:
                m = msg['method']
                if type(m)!=unicode:
                    msg['method'] = unicode(m,'ascii')
            except KeyError:
                pass

    def _send_and_response_message(self, addr, msg):
        self._namefixer(msg)
        res = send_and_receive_response(addr, msg, 30) # manual timeout !!!!!  fix it!
        return res


    def __call__(self, *args, **kwargs):
        raise NotImplemented("can't call proxy")



class RawProxy(GenericProxy):
    """
    Internal use proxy (by async worker)
    """
    def _send_and_response(self, addr, msg):
        """
        Send request and return raw message
        """
        self._namefixer(msg)
        return send_and_receive(addr, msg, 30) # manual timeout !!!!!  fix it!



    def sync_call(self, service, method, context, args, kwargs):
        addr = self._find_worker(service)
        msg = {
            "message" : messages.SYNC_CALL,
            "service" : service,
            "method"  : method,
            "context" : context,
            "args"    : args,
            "kwargs"  : kwargs
        }
        return self._send_and_response(addr, msg)




class SyncProxy(GenericProxy):

    def __call__(self, *args, **kwargs):
        """
        Wywołanie synchroniczne jest wykonywane natychmiast.
        """
        method = self._names
        context = self._context
        #if self._allow_method_mocking:
        #    m = '.'.join(method)
        #    if m in self._mock_methods:
        #        return self._mock_methods[m](*args, **kwargs)
        addr = self._find_worker(method[0])
        # zbudowanie komunikatu
        msg = {
            "message" : messages.SYNC_CALL,
            "service" : method[0],
            "method" : ".".join( method[1:] ),
            "context" : context,
            "args" : args,
            "kwargs" : kwargs
        }
        #LOG.debug("Client is about to send this message: %r" % msg)
        # wysłanie żądania
        return self._send_and_response_message(addr, msg)



class AsyncProxy(GenericProxy):

    def __call__(self, *args, **kwargs):
        method = self._names
        context = self._context
        addr = self._find_worker("async")#[settings.ASYNC_DAEMON_SERVICE, "register_task"])
        # zbudowanie komunikatu
        msg = {
            "message" : messages.ASYNC_CALL,
            "method" : ".".join(method),
            "context" : context,
            "args"    : args,
            "kwargs"  : kwargs
        }
        return self._send_and_response_message(addr, msg)



class ControlProxy(GenericProxy):

    def __call__(self, *args, **kwargs):
        method = self._names
        context = self._context
        msg = {
            "message" : messages.CTL_CALL,
            "method" : ".".join(method),
            "context" : context,
            "args" : args,
            "kwargs" : kwargs
        }
        # wysłanie żądania
        kasaya = KasayaLocalClient()
        msgbody = kasaya.control_task(msg)
        msg = msgbody['message']
        if msg==messages.RESULT:
            return msgbody['result']


class TransactionProxy(SyncProxy):
    pass
