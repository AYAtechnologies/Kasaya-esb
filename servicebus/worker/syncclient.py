#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from gevent_zeromq import zmq
from servicebus.protocol import serialize, deserialize, messages
from servicebus.lib import LOG


class SyncClient(object):
    def __init__(self, servicename, address):
        self.srvname = servicename
        self.addr = address
        # connect to zmq
        self.ctx = zmq.Context()
        self.sync_sender = self.ctx.socket(zmq.REQ)
        #self.sync_sender.setsockopt(zmq.LINGER, 1000) # two seconds
        #self.sync_sender.setsockopt(zmq.HWM, 8) # how many messages buffer
        self.sync_sender.connect('ipc://'+settings.SOCK_QUERIES)

    def notify_start(self):
        LOG.debug("Sending notification on start to sync daemon. Service [%s] on address [%s]" % (self.srvname, self.addr))
        msg = {
            "message" : messages.WORKER_JOIN,
            "addr" : self.addr,
            "service" : self.srvname,
            }
        self.sync_sender.send( serialize(msg) )
        self.sync_sender.recv()

    def notify_stop(self):
        LOG.debug("Sending notification on stop. Address [%s]" % self.addr)
        msg = {
            "message" : messages.WORKER_LEAVE,
            "addr" : self.addr,
            }
        self.sync_sender.send( serialize(msg) )
        self.sync_sender.recv()

    def send_raw(self, msg):
        self.sync_sender.send( serialize(msg) )
        self.sync_sender.recv()

    def close(self):
        self.sync_sender.close()

