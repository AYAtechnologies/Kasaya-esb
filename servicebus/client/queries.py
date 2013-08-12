#!/usr/bin/env python
#coding: utf-8
from servicebus.protocol import serialize, deserialize, messages
from servicebus.conf import settings
import zmq


class SyncQuery(object):

    def __init__(self):
        self.ctx = zmq.Context()
        self.queries = self.ctx.socket(zmq.REQ)
        print settings.SOCK_QUERIES
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


SyncDQuery = SyncQuery()


