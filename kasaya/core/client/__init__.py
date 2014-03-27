#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .context import Context
from .proxies import SyncExec, AsyncExec, TransactionExec, ControlExec
from .asyncresult import AsyncResult

__all__ = ("sync", "async", "trans", "control", "AsyncResult", "Context")

sync = SyncExec()
async = AsyncExec()
trans = TransactionExec()
control = ControlExec()

# remove unneccessary imports
del SyncExec, AsyncExec, TransactionExec, ControlExec
