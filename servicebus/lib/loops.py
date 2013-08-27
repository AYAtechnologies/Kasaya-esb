#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from servicebus.conf import settings
#from servicebus.binder import bind_socket_to_port_range,
from gevent_zeromq import zmq
import gevent
#from syncclient import SyncClient
from servicebus.protocol import serialize, deserialize, messages



class BaseLoop(object):

    def __init__(self, connector, context=None):
        self.is_running = True
        self._msgdb = {}
        if context is None:
            self.__context = zmq.Context()
        else:
            self.__context = context
        self.SOCK, self.address = connector(self.__context)

    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        self.is_running = False

    def close(self):
        self.SOCK.close()

    def register_message(self, message, func):
        self._msgdb[message]=func

    def loop(self):
        raise NotImplemented("loop method must be implemented when BaseLoop is inherited")



class PullLoop(BaseLoop):

    def loop(self):
        while self.is_running:
            # receive data
            msgdata = self.SOCK.recv()

            # deserialize
            try:
                msgdata = deserialize(msgdata)
                msg = msgdata['message']
            except Exception as e:
                continue

            # find handler
            try:
                handler = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored silently
                print "unknown message ", msg
                continue

            # run handler
            try:
                handler(msgdata)
            except Exception as e:
                # ignore exceptions
                continue



class RepLoop(BaseLoop):
    """
    pętla nasłuchująca na sockecie typu REP (odpowiedzi na REQ).
    """

    def send_noop(self):
        noop = {"message":messages.NOOP}
        self.SOCK.send( serialize(noop) )

    def send(self, message):
        self.SOCK.send( serialize(message) )


    def loop(self):
        while self.is_running:
            # receive data
            msgdata = self.SOCK.recv()

            # deserialize
            try:
                msgdata = deserialize(msgdata)
                msg = msgdata['message']
            except Exception as e:
                self.send_noop()
                print "unknown message ", msg
                continue

            # find handler
            try:
                handler = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored silently
                self.send_noop()
                continue

            # run handler
            try:
                result = handler(msgdata)
            except Exception as e:
                result = None

            # send result
            if result is None:
                self.send_noop()
            else:
                self.send(result)


