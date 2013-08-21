#!/usr/bin/env python
#coding: utf-8
from servicebus.protocol import messages, serialize, deserialize
from servicebus.client.queries import SyncDQuery
from servicebus.binder import get_bind_address, bind_socket_to_port_range
from servicebus.conf import settings
from servicebus import exceptions
from gevent_zeromq import zmq


class WorkerCaller(object):

    def __init__(self):
        self.context = zmq.Context()


    def send_request_to_worker(self, target, msg):
        self.REQUESTER = self.context.socket(zmq.REQ)
        self.REQUESTER.connect(target)
        self.REQUESTER.send( serialize(msg) )
        res = self.REQUESTER.recv()
        res = deserialize(res)
        self.REQUESTER.close()
        return res


worker_caller = WorkerCaller()


def find_worker(method):
    srvce = method[0]
    msg = SyncDQuery.query( srvce )
    if not msg['message']==messages.WORKER_ADDR:
        raise exceptions.ServiceBusException("Wrong response from sync server")
    if msg['addr'] is None:
        raise exceptions.ServiceNotFound("No service %s found" % srvce)
    return msg['addr']



def execute_sync_task(method, authinfo, timeout, args, kwargs, addr = None):
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
        "authinfo" : authinfo,
        "args" : args,
        "kwargs" : kwargs
    }
    # wysłanie żądania
    print "Sync task: ", addr, msg
    #worker_caller = WorkerCaller()
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
        raise Exception("Wrong worker response")


def register_async_task(method, authinfo, timeout, args, kwargs):
    """
    Wywołanie asynchroniczne powinno zostać zapisane w bazie i zostać wykonane
    w tle. Wynikiem funkcji powinien być identyfikator zadania wg którego można
    sprawdzić jego status.
    """
    # odpytanie o worker który wykona zadanie
    addr = find_worker(["async_daemon", "register"])
    # zbudowanie komunikatu
    msg = {
        "message" : messages.ASYNC_CALL,
        "service" : "async_daemon",
        "method" : ["register"],
        "original_method": method,
        "authinfo" : authinfo,
        "args" : args,
        "kwargs" : kwargs
    }
    # wysłanie żądania
    print "Async task: ", addr, msg
    worker_caller = WorkerCaller()
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
        raise Exception("Wrong worker response")


def get_async_result(task_id, authinfo, timeout=0):
    #execute_sync_task(method, authinfo, timeout, args, kwargs, addr = None)
    m = ["async_daemon", "get_result"]
    return execute_sync_task(m, authinfo, timeout, [task_id], {})
