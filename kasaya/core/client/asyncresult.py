#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
#from kasaya.core.protocol import messages
#from kasaya.conf import settings
#from .generic_proxy import GenericProxy

#__all__ = ("SyncProxy", "AsyncProxy", "ControlProxy", "TransactionProxy", "async_result")


def async_result(task_id, context):
    #execute_sync_task(method, context, args, kwargs, addr = None)
    m = [settings.ASYNC_DAEMON_SERVICE, "get_task_result"]
    s = SyncProxy()
    s._names = m
    s._context = context
    return s(task_id)
