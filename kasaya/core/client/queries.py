#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import messages, Serializer
from kasaya.core.lib.comm import send_and_receive
from kasaya.core import SingletonCreator
from kasaya.conf import settings
import zmq.green as zmq


class SyncQuery(object):
    """
    Class which realises queries to kasayad daemon asking about available daemons.
    """
    __metaclass__ = SingletonCreator

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
        zadanie tego typu jest wysyłane do serwera kasayad nie do workera!
        """
        return send_and_receive(self.ctx, self.addr, msg)
        #self.queries.send( serialize(msg) )
        #res = self.queries.recv()
        #res = deserialize(res)
        #return res


