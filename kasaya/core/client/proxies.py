#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import messages
from kasaya.conf import settings
from kasaya.core.protocol.sendrecv import send_and_receive_response, send_and_receive
from kasaya.core import exceptions
from kasaya.core.lib import LOG
from kasaya.core.protocol.kasayad_client import KasayaLocalClient, WorkerFinder
from kasaya.core.client.asyncresult import AsyncResult
from .context import Context
import weakref


# is gevent available?
try:
    import gevent
    _gev = True
except ImportError:
    _gev = False


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
        res = send_and_receive_response(addr, msg)
        return res




class RawProxy(GenericProxy):
    """
    Internal proxy (used by async worker).
    It's used to send manually created messages to workers.
    """
    def _send_and_response(self, addr, msg):
        """
        Send request and return raw message
        """
        self._namefixer(msg)
        return send_and_receive(addr, msg)

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
        if self._context is None:
            self._context = Context()
        method = self._names
        if self._context['depth'] == 0:
            raise exceptions.MaximumDepthLevelReached("Maximum level of requests depth reached")
        #print ("ID",id(context))
        #print ("IDmethod",id(method))
        #if self._allow_method_mocking:
        #    m = '.'.join(method)
        #    if m in self._mock_methods:
        #        return self._mock_methods[m](*args, **kwargs)
        addr = self._find_worker(method[0])
        # zbudowanie komunikatu
        self._context['depth'] -= 1
        msg = {
            "message" : messages.SYNC_CALL,
            "service" : method[0],
            "method" : ".".join( method[1:] ),
            "context" : self._context,
            "args" : args,
            "kwargs" : kwargs
        }
        # wysłanie żądania
        try:
            return self._send_and_response_message(addr, msg)
        finally:
            self._context['depth'] += 1



class AsyncProxy(GenericProxy):

    def __call__(self, *args, **kwargs):
        if self._context is None:
            self._context = Context()
        addr = self._find_worker("async")#[settings.ASYNC_DAEMON_SERVICE, "register_task"])
        # zbudowanie komunikatu
        msg = {
            "message" : messages.ASYNC_CALL,
            "method" : ".".join(self._names),
            "context" : self._context,
            "args"    : args,
            "kwargs"  : kwargs
        }
        aid = self._send_and_response_message(addr, msg)
        return AsyncResult(aid)



class ControlProxy(GenericProxy):

    def __call__(self, *args, **kwargs):
        if self._context is None:
            self._context = Context()
        if self._context['depth'] == 0:
            raise exceptions.MaximumDepthLevelReached
        msg = {
            "message" : messages.CTL_CALL,
            "method" : ".".join(self._names),
            "context" : self._context,
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



# task caller

class _ExecBase(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, context=None):
        """
        Context parameter should be used only when creating new root context or when overriding existing context.
        """
        # context override
        if context is None:
            self.__ctx_ovrr = None
        else:
            self.__ctx_ovrr = context

    def __get_current_context(self):
        """
        Return current request context
        """
        if not self.__ctx_ovrr is None:
            return self.__ctx_ovrr
        global _gev
        if not _gev:
            return None
        grnlt = gevent.getcurrent()
        try:
            # current greenlet context
            return weakref.proxy( grnlt.context )
        except AttributeError:
            # no context
            pass
        return None

    def __getattr__(self, itemname):
        """
        create proxy instance and return it back as result
        """
        if itemname[0]=="_":
            raise AttributeError("No attribute %s in %r" % (itemname,self) )
        proxy = self._create_proxy()
        proxy._context = self.__get_current_context()
        proxy._names.append(itemname)
        return proxy


class SyncExec(_ExecBase):
    def _create_proxy(self):
        return SyncProxy()

class AsyncExec(_ExecBase):
    def _create_proxy(self):
        return AsyncProxy()

class TransactionExec(_ExecBase):
    def _create_proxy(self):
        return TransactionProxy()

class ControlExec(_ExecBase):
    def _create_proxy(self):
        return ControlProxy()
