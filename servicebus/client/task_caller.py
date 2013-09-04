#!/usr/bin/env python
#coding: utf-8

#from servicebus.lib.binder import get_bind_address, bind_socket_to_port_range
from servicebus.middleware.core import MiddlewareCore




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




