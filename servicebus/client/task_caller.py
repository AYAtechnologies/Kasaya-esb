#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.protocol import messages, serialize, deserialize
from servicebus.client.queries import SyncDQuery
#from servicebus.lib.binder import get_bind_address, bind_socket_to_port_range
from servicebus.middleware.core import MiddlewareCore
from servicebus.conf import settings
from servicebus import exceptions
from gevent_zeromq import zmq


class Caller(MiddlewareCore):

    def __init__(self):
        super(Caller, self).__init__()
        self.context = zmq.Context()

    def send_request_to_worker(self, target, msg):
        msg = self.prepare_message(msg) # _middleware hook
        if "context" not in msg:
            msg["context"] = {}
        REQUESTER = self.context.socket(zmq.REQ)
        REQUESTER.connect(target)
        REQUESTER.send(serialize(msg))
        res = REQUESTER.recv()
        res = deserialize(res)
        REQUESTER.close()
        res = self.postprocess_message(res) # _middleware hook
        return res


worker_caller = Caller()


def find_worker(method):
    srvce = method[0]
    msg = SyncDQuery.query( srvce )
    if not msg['message']==messages.WORKER_ADDR:
        raise exceptions.ServiceBusException("Wrong response from sync server")
    if msg['ip'] is None:
        raise exceptions.ServiceNotFound("No service %s found" % srvce)
    addr = "tcp://%s:%i/" % ( msg['ip'],msg['port'] )
    return addr



def execute_sync_task(method, context, args, kwargs, addr = None):
    """
    Wywołanie synchroniczne jest wykonywane natychmiast.
    """
    # odpytanie o worker który wykona zadanie
    if addr is None:
        addr = find_worker(method)
    # zbudowanie komunikatu
    msg = {
        "message" : messages.SYNC_CALL,
        "service" : method[0],
        "method" : method[1:],
        "context" : context,
        "args" : args,
        "kwargs" : kwargs
    }
    # wysłanie żądania
    #print "Sync task: ", addr, msg
    msg = worker_caller.send_request_to_worker(addr, msg)
    if msg['message']==messages.RESULT:
        return msg['result']
    elif msg['message']==messages.ERROR:
        # internal service bus error
        if msg['internal']:
            raise Exception(msg['info'])
        # error during task execution
        e = Exception(msg['info'])
        e.traceback = msg['traceback']
        print "-"*10
        print msg['traceback']
        raise e
    else:
        raise Exception("Wrong worker response")



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
    msg = worker_caller.send_request_to_worker(addr, msg)
    if msg['message']==messages.RESULT:
        return msg['result']
    elif msg['message']==messages.ERROR:
        # internal service bus error
        if msg['internal']:
            raise Exception(msg['info'])
        # error during task execution
        e = Exception(msg['info'])
        e.traceback = msg['traceback']
        raise e
    else:
        raise Exception("Wrong worker response", str(msg))


def async_result(task_id, context):
    #execute_sync_task(method, context, args, kwargs, addr = None)
    m = [settings.ASYNC_DAEMON_SERVICE, "get_task_result"]
    return execute_sync_task(m, context, [task_id], {})
