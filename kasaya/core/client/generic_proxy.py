#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.middleware.core import MiddlewareCore
from kasaya.core.protocol import messages, Serializer
from kasaya.core.client.queries import SyncQuery
from kasaya.core.lib.comm import send_and_receive_response
from kasaya.core import exceptions
import zmq.green as zmq

# on python 2 we need to fix some strings to unicode
# this is required to work with python 3 workers called with python 2 clients
import sys
_namefix = sys.version_info<(3,0)
del sys



class GenericProxy(MiddlewareCore):
    """
    Ta klasa przekształca normalne pythonowe wywołanie z kropkami:
    a.b.c.d
    na pojedyncze wywołanie z listą użytych metod ['a','b','c','d']
    """

    def __init__(self, top=None):
        super(GenericProxy, self).__init__()
        self.sync_query = SyncQuery()
        self._top = top
        self._names = []
        self._method = None
        self._context = []
        self._z_context = zmq.Context()

    def initialize(self, method, context):
        self._names = method
        self._context = context
        self.addr = self.find_worker(method)

    def __getattr__(self, itemname):
        if itemname.startswith("_"):
            # zbędne
            return super(GenericProxy, self).__getattribute__(itemname)
        #M = GenericProxy(top=self._top)
        self._names.append(itemname)
        return self

    def find_worker(self, method):
        srvce = method[0]
        msg = self.sync_query.query( srvce )
        if not msg['message']==messages.WORKER_ADDR:
            raise exceptions.ServiceBusException("Wrong response from sync server")
        if msg['ip'] is None:
            raise exceptions.ServiceNotFound("No service %s found" % srvce)
        addr = "tcp://%s:%i/" % ( msg['ip'],msg['port'] )
        return addr

    def _send_message(self, addr, msg):
        global _namefix
        if _namefix:
            msg['service'] = unicode(msg['service'],'ascii')
            msg['method'] = [ unicode(m,"ascii") for m in msg['method']]
        msg = self.prepare_message(msg) # _middleware hook
        res = send_and_receive_response(self._z_context, addr, msg, 30)
        res = self.postprocess_message(res) # _middleware hook
        return res

    def __call__(self, *args, **kwargs):
        print ("generic proxy", self._names)
        raise NotImplemented


