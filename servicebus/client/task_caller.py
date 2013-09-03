#!/usr/bin/env python
#coding: utf-8

#from servicebus.lib.binder import get_bind_address, bind_socket_to_port_range
from servicebus.middleware.core import MiddlewareCore






#
#
#
# class FuncProxyOld(object):
#     """
#     Ta klasa przekształca normalne pythonowe wywołanie z kropkami:
#     a.b.c.d
#     na pojedyncze wywołanie z listą użytych metod ['a','b','c','d']
#     """
#
#     def __init__(self, top=None):
#         self._top = top
#         self._names = []
#         self._method = None
#
#     def __getattribute__(self, itemname):
#         if itemname.startswith("_"):
#             return super(FuncProxy, self).__getattribute__(itemname)
#         M = FuncProxy(top=self._top)
#         self._top._names.append(itemname)
#         return M
#
#     def __contains__(self, name):
#         """
#         Wszystkie atrybuty są możliwe, bo każdy atrybut może istnieć
#         po stronie workera, dlatego zawsze True. W sumie nie wiem czy to
#         jest potrzebna metoda, ale niech na razie będzie.
#         """
#         return True
#
#     def __call__(self, *args, **kwargs):
#         top = self._top
#         m = top._method
#         del self._top   # pomagamy garbage collectorowi
#         if m=="sync":
#             print "sync"
#             # return execute_sync_task(
#             #     top._names,
#             #     top._context,
#             #     args,
#             #     kwargs)
#         elif m=="async":
#             return register_async_task(
#                 top._names,
#                 top._context,
#                 args,
#                 kwargs)
#         elif m=="ctl":
#             return execute_control_task(
#                 top._names,
#                 top._context,
#                 args,
#                 kwargs)
#         elif m=="trans":
#             print "trans", top, top._names, args, kwargs
#             return execute_sync_task(
#                 top._names,
#                 top._context,
#                 args,
#                 kwargs)
#         else:
#             raise Exception("Unknown call type")




def execute_control_task(method, context, args, kwargs, addr = None):
    """
    Wywołanie zadania kontrolnego wysyłane jest do syncd a nie do workerów!
    """
    # zbudowanie komunikatu
    msg = {
        "message" : messages.CTL_CALL,
        "method" : method,
        "context" : context,
        "args" : args,
        "kwargs" : kwargs
    }
    # wysłanie żądania
    #print "Control task: ", msg
    msgbody = SyncDQuery.control_task(msg)
    msg = msgbody['message']
    if msg==messages.RESULT:
        return msgbody['result']




def register_async_task(method, context, args, kwargs):
    """
    Wywołanie asynchroniczne powinno zostać zapisane w bazie i zostać wykonane
    w tle. Wynikiem funkcji powinien być identyfikator zadania wg którego można
    sprawdzić jego status.
    """
    # odpytanie o worker który wykona zadanie
    addr = find_worker([settings.ASYNC_DAEMON_SERVICE, "register_task"])
    # zbudowanie komunikatu
    msg = {
        "message" : messages.SYSTEM_CALL,
        "service" : settings.ASYNC_DAEMON_SERVICE,
        "method" : ["register_task"],
        "original_method": method,
        "context" : context,
        "args" : args,
        "kwargs" : kwargs
    }
    # wysłanie żądania
    print "Async task: ", addr, msg



def async_result(task_id, context):
    #execute_sync_task(method, context, args, kwargs, addr = None)
    m = [settings.ASYNC_DAEMON_SERVICE, "get_task_result"]
    return execute_sync_task(m, context, [task_id], {})
