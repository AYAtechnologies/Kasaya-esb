#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .exec_context import SyncExec, AsyncExec, TransactionExec, ControlExec
from .context import Context
from .asyncresult import AsyncResult
#from .proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy

__all__ = ("sync", "async", "trans", "control", "AsyncResult", "Context")

sync = SyncExec()
async = AsyncExec()
trans = TransactionExec()
control = ControlExec()

# remove unneccessary imports
del SyncExec, AsyncExec, TransactionExec, ControlExec
