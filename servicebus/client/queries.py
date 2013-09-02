#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.protocol import serialize, deserialize, messages
from servicebus.conf import settings
from gevent_zeromq import zmq


class SyncQuery(object):

    def __init__(self):
        self.ctx = zmq.Context()
        self.queries = self.ctx.socket(zmq.REQ)
        self.queries.connect('ipc://'+settings.SOCK_QUERIES)

    def query(self, service):
        """
        odpytuje przez zeromq lokalny nameserver o to gdzie realizowany
        jest serwis o żądanej nazwie
        """
        msg = {'message':messages.QUERY, 'service':service}
        self.queries.send(serialize(msg))
        res = self.queries.recv()
        res = deserialize(res)
        return res

    def control_task(self, msg):
        """
        zadanie tego typu jest wysyłane do serwera syncd nie do workera!
        """
        self.queries.send( serialize(msg) )
        res = self.queries.recv()
        res = deserialize(res)
        return res


SyncDQuery = SyncQuery()


