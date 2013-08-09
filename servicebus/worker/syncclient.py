#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
import zmq
from servicebus.protocol import serialize, deserialize


class SyncClient(object):
    def __init__(self, servicename, address):
        self.srvname = servicename
        self.addr = address
        # connect to zmq
        self.ctx = zmq.Context()
        self.sync_sender = self.ctx.socket(zmq.PUSH)
        self.sync_sender.connect('ipc://'+settings.SOCK_LOCALWORKERS)

    def notify_start(self):
        msg = {"message":"worker-connect"}
        msg['addr'] = self.addr
        msg['service'] = self.srvname
        self.sync_sender.send( serialize(msg) )

    def notify_stop(self):
        msg = {"message":"worker-disconnect"}
        msg['addr'] = self.addr
        self.sync_sender.send( serialize(msg) )


