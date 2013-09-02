#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from gevent_zeromq import zmq
from servicebus.protocol import serialize, deserialize, messages


class SyncClient(object):

    def __init__(self, servicename, ip, port, uuid):
        self.srvname = servicename
        self.__addr = ip
        self.__port = port
        self.__pingmsg = {
            "message" : messages.WORKER_LIVE,
            "addr" : ip,
            "port" : port,
            "uuid" : uuid,
            "service" : servicename
        }
        # connect to zmq
        self.ctx = zmq.Context()
        self.sync_sender = self.ctx.socket(zmq.REQ)
        #self.sync_sender.setsockopt(zmq.LINGER, 1000) # two seconds
        #self.sync_sender.setsockopt(zmq.HWM, 8) # how many messages buffer
        self.sync_sender.connect('ipc://'+settings.SOCK_QUERIES)

    def notify_live(self):
        self.sync_sender.send( serialize(self.__pingmsg) )
        self.sync_sender.recv()

    def notify_stop(self):
        msg = {
            "message" : messages.WORKER_LEAVE,
            "ip" : self.__addr,
            "port" : self.__port,
            }
        self.sync_sender.send( serialize(msg) )
        self.sync_sender.recv()

    def send_raw(self, msg):
        self.sync_sender.send( serialize(msg) )
        self.sync_sender.recv()

    def close(self):
        self.sync_sender.close()

