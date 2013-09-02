#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from servicebus.conf import settings
#from servicebus.binder import bind_socket_to_port_range,
from gevent_zeromq import zmq
import gevent
#from syncclient import SyncClient
from servicebus.protocol import serialize, deserialize, messages
from servicebus.lib import LOG
import traceback,sys



class BaseLoop(object):

    def __init__(self, connector, context=None):
        self.is_running = True
        self._msgdb = {}
        if context is None:
            self.__context = zmq.Context()
        else:
            self.__context = context
        # bind to socket
        self.SOCK, addr = connector(self.__context)
        self.address = addr
        addr = addr.split(":")
        if len(addr)==3:
            self.ip = addr[1].lstrip("/")
            self.port = int(addr[2])


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
    """
    PullLoop is receiving only loop for messages.
    """

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
            except Exception as e:
                self.send_noop()
                LOG.warning("Message deserialisation error")
                LOG.debug("Broken message body dump in hex (only first 1024 bytes):\n%s" % msgdata[:1024].encode("hex"))
                continue

            try:
                msg = msgdata['message']
            except KeyError:
                LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
                continue

            # find handler
            try:
                handler = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored
                self.send_noop()
                LOG.warning("Unknown message received [%s]" % msg)
                LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue

            # run handler
            try:
                result = handler(msgdata)
            except Exception as e:
                # log exception details
                excname = e.__class__.__name__
                # traceback
                tback = traceback.format_exc()
                try:
                    tback = unicode(tback, "utf-8")
                except:
                    tback = repr(tback)
                # error message
                errmsg = e.message
                try:
                    errmsg = unicode(errmsg, "utf-8")
                except:
                    errmsg = repr(errmsg)
                # log & clean
                LOG.error("Exception [%s] when processing message [%s]. Message: %s." % (excname, msg, errmsg) )
                LOG.debug("Message dump: %s" % repr(msgdata) )
                LOG.debug(tback)
                del excname, tback, errmsg
                result = None

            # send result
            if result is None:
                self.send_noop()
            else:
                self.send(result)
