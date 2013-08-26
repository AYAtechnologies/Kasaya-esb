#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from gevent_zeromq import zmq
from servicebus.protocol import serialize, deserialize, messages


class SyncClient(object):
    def __init__(self, servicename, address):
        self.srvname = servicename
        self.addr = address
        # connect to zmq
        self.ctx = zmq.Context()
        self.sync_sender = self.ctx.socket(zmq.PUSH)
        self.sync_sender.connect('ipc://'+settings.SOCK_LOCALWORKERS)

    def notify_start(self):
        msg = {
            "message" : messages.WORKER_JOIN,
            "addr" : self.addr,
            "service" : self.srvname,
            }
        self.sync_sender.send( serialize(msg) )

    def notify_stop(self):
        msg = {
            "message" : messages.WORKER_LEAVE,
            "addr" : self.addr,
            }
        self.sync_sender.send( serialize(msg) )

    def close(self):
        self.sync_sender.close()

