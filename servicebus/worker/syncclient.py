#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from gevent_zeromq import zmq
from servicebus.protocol import serialize, deserialize, messages
import gevent
from gevent.coros import Semaphore


class TooLong(Exception): pass


class SyncClient(object):
    """
    SyncClient jest używany do komunikacji workera z lokalnym serverem syncd.
    """

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
        self.connect()
        self.SEMA = Semaphore()

    def connect(self):
        self.ctx = zmq.Context()
        self.sync_sender = self.ctx.socket(zmq.REQ)
        #self.sync_sender.setsockopt(zmq.LINGER, 1000) # two seconds
        #self.sync_sender.setsockopt(zmq.HWM, 8) # how many messages buffer
        self.sync_sender.connect( 'ipc://'+settings.SOCK_QUERIES )

    def disconnect(self):
        #self.sync_sender.disconnect(self.__sockaddr)
        self.sync_sender.close()
        del self.sync_sender
        del self.ctx

    def send(self, msg):
        """
        Send message request to syncd. Return True if success, False if delivery failed.
        """
        self.sync_sender.send( serialize(msg) )
        try:
            with gevent.Timeout(settings.SYNC_REPLY_TIMEOUT, TooLong):
                self.sync_sender.recv()
                return True
        except TooLong:
            self.SEMA.acquire()
            self.disconnect()
            self.connect()
            self.SEMA.release()
            return False

    def notify_live(self):
        return self.send(self.__pingmsg)

    def notify_stop(self):
        msg = {
            "message" : messages.WORKER_LEAVE,
            "ip" : self.__addr,
            "port" : self.__port,
            }
        self.send(msg)

    def close(self):
        self.sync_sender.close()
