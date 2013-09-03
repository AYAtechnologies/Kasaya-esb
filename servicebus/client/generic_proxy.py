#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

from servicebus.middleware.core import MiddlewareCore
from servicebus.protocol import messages, serialize, deserialize
from servicebus.client.queries import SyncDQuery
from servicebus import exceptions
from gevent_zeromq import zmq


class GenericProxy(MiddlewareCore):
    """
    Ta klasa przekształca normalne pythonowe wywołanie z kropkami:
    a.b.c.d
    na pojedyncze wywołanie z listą użytych metod ['a','b','c','d']
    """

    def __init__(self, top=None):
        super(GenericProxy, self).__init__()
        self._top = top
        self._names = []
        self._method = None
        self._z_context = zmq.Context()

    def __getattr__(self, itemname):
        if itemname.startswith("_"):
            return super(GenericProxy, self).__getattr__(itemname)
        #M = GenericProxy(top=self._top)
        self._names.append(itemname)
        return self

    def _find_worker(self, method):
        srvce = method[0]
        msg = SyncDQuery.query( srvce )
        if not msg['message']==messages.WORKER_ADDR:
            raise exceptions.ServiceBusException("Wrong response from sync server")
        if msg['ip'] is None:
            raise exceptions.ServiceNotFound("No service %s found" % srvce)
        addr = "tcp://%s:%i/" % ( msg['ip'],msg['port'] )
        return addr

    def _send_message(self, addr, msg):
        msg = self._send_request_to_worker(addr, msg)
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

    def _send_request_to_worker(self, target, msg):
        msg = self.prepare_message(msg) # _middleware hook
        if "context" not in msg:
            msg["context"] = {}
        REQUESTER = self._z_context.socket(zmq.REQ)
        REQUESTER.connect(target)
        REQUESTER.send(serialize(msg))
        res = REQUESTER.recv()
        res = deserialize(res)
        REQUESTER.close()
        res = self.postprocess_message(res) # _middleware hook
        return res

    def __call__(self, *args, **kwargs):
        print "generic proxy", self._names
        raise NotImplemented


