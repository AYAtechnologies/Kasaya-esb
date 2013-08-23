#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from servicebus.conf import settings
#from servicebus.binder import bind_socket_to_port_range,
from gevent_zeromq import zmq
import gevent
#from syncclient import SyncClient
from servicebus.protocol import serialize, deserialize



class RepLoop(object):
    """
    pętla nasłuchująca na sockecie typu REP (odpowiedzi na REQ).
    """

    def __init__(self, connector):
        self.__running = True
        self.__msgdb = {}
        self.__context = zmq.Context()
        self.SOCK = connector(self.__context)


    def register_message(self, message, func):
        self.__msgdb[message]=func


    def send_noop(self):
        noop = {"message":messages.NOOP}
        self.SOCK.send( serialize(noop) )

    def send(self, message):
        self.SOCK.send( serialize(message) )


    def loop(self):
        while self.__running:
            # receive data
            msgdata = self.SOCK.recv()
            try:
                msgdata = deserialize(msgdata)
                msg = msgdata['message']
            except Exception as e:
                self.send_noop()
                continue

            # find handler
            try:
                handler = self.__msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored silently
                self.send_noop()
                continue

            # run handler
            try:
                result = handler(msgdata)
            except Exception as e:
                result = None

            if result is None:
                self.send_noop()
            else:
                self.send(result)


    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        self.__running = False


    def close(self):
        self.SOCK.close()



