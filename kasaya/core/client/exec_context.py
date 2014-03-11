#encoding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy
from .context import Context
import weakref

class _ExecBase(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, context=None):
        if isinstance(context, Context):
            # context ist given directly
            # this means that uderlying context derived from remote
            # client with task request, will be not used here.
            self._context = weakref.proxy(context)
        elif context is None:
            # context is none, this means that context can be empty (anonymous),
            # or derived from task request via greenlet locals.
            # TODO: greenlet locals check
            self._context = None
        else:
            raise Exception("context parameter can be only Context instance or None")

    def __getattr__(self, itemname):
        """
        When called immediatelly without context:
            sync.service.function()
        we must create proxy instance and return it back as result
        """
        proxy = self._create_proxy()
        proxy._context = self._context
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
