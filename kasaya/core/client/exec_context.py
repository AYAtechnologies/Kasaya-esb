#encoding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy
from .context import Context
import weakref
try:
    import gevent
    _gev = True
except ImportError:
    _gev = False


class _ExecBase(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, context=None):
        """
        context parameter should be used only when creating new root context or when overriding existing context
        """
        # context override
        if context is None:
            self.__ctx_ovrr = None
        else:
            self.__ctx_ovrr = context

    def _get_current_context(self):
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
        proxy._context = self._get_current_context()
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
