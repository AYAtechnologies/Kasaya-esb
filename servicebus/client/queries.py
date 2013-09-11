#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.protocol import serialize, deserialize, messages
from servicebus.conf import settings
from gevent_zeromq import zmq
from servicebus.lib.comm import send_and_receive


class SyncQuery(object):

    def __init__(self):
        self.ctx = zmq.Context()
        #self.queries = self.ctx.socket(zmq.REQ)
        self.addr = 'ipc://'+settings.SOCK_QUERIES

    def query(self, service):
        """
        odpytuje przez zeromq lokalny nameserver o to gdzie realizowany
        jest serwis o żądanej nazwie
        """
        msg = {'message':messages.QUERY, 'service':service}
        return send_and_receive(self.ctx, self.addr, msg)

    def control_task(self, msg):
        """
        zadanie tego typu jest wysyłane do serwera syncd nie do workera!
        """
        return send_and_receive(self.ctx, self.addr, msg)
        #self.queries.send( serialize(msg) )
        #res = self.queries.recv()
        #res = deserialize(res)
        #return res


SyncDQuery = SyncQuery()


