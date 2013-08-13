#!/usr/bin/env python
#coding: utf-8
from servicebus.protocol import messages, serialize, deserialize
from servicebus.client.queries import SyncDQuery
from servicebus.binder import get_bind_address, bind_socket_to_port_range
from servicebus.conf import settings
import zmq


class WorkerCaller(object):

    def __init__(self):
        self.context = zmq.Context()
        self.REQUESTER = self.context.socket(zmq.REQ)


    def send_request_to_worker(self, target, msg):
        self.REQUESTER.connect(target)
        self.REQUESTER.send( serialize(msg) )
        res = self.REQUESTER.recv()
        res = deserialize(res)
        return res

worker_caller = WorkerCaller()


def find_worker(method):
    srvce = method[0]
    msg = SyncDQuery.query( srvce )
    if not msg['message']==messages.WORKER_ADDR:
        raise Exception("Wrong response from syncd")
    if msg['addr'] is None:
        raise Exception("No service %s found" % srvce)
    return msg['addr']



def execute_sync_task(method, authinfo, timeout, args, kwargs):
    """
    Wywołanie synchroniczne jest wykonywane natychmiast.
    """
    # odpytanie o worker który wykona zadanie
    addr = find_worker(method)
    # zbudowanie komunikatu
    msg = {
        "message":messages.SYNC_CALL,
        "service":method[0],
        "method":method[1:],
        "authinfo":authinfo,
        "args":args,
        "kwargs":kwargs
    }
    # wysłanie żądania
    result = worker_caller.send_request_to_worker(addr, msg)
    return result



def register_async_task(method, authinfo, timeout, args, kwargs):
    """
    Wywołanie asynchroniczne powinno zostać zapisane w bazie i zostać wykonane
    w tle. Wynikiem funkcji powinien być identyfikator zadania wg którego można
    sprawdzić jego status.
    """
    return
    print "ASYNCHRONOUS",
    print "AUTHINFO:", authinfo, "TIMEOUT:", timeout,
    print "method:", method
    worker = SyncDQuery.query( method[0] )
#    print "Worker:",worker
    print "ARGS:",args,
    print "KWARGS:",kwargs
    print
#    return "fake-id"
