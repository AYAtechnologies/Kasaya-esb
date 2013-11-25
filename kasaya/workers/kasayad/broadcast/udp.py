#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals
from kasaya.conf import settings
from gevent import socket
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib import LOG
from kasaya.core.exceptions import NotOurMessage
from kasaya.core.events import emit
import traceback


class UDPLoop(object):

    def __init__(self):
        self.is_running = True
        self.own_ip = None
        self.ID = None
        self._msgdb = {}
        self.port = settings.BROADCAST_PORT
        self.SOCK = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.SOCK.bind(('',settings.BROADCAST_PORT))
        self.SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.serializer = Serializer()

    def set_own_ip(self, ip):
        self.own_ip = ip

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
        while self.is_running:
            # receive data
            msgdata, addr = self.SOCK.recvfrom(4096)
            # skip own broadcast messages
            if addr[0]==self.own_ip:
                continue
            # deserialize
            try:
                msgdata, repreq = self.serializer.deserialize(msgdata)
            except NotOurMessage:
                continue
            except Exception:
                LOG.warning("Message from broadcast deserialisation error")
                LOG.debug("Broken message body dump in hex (only first 1024 bytes):\n%s" % msgdata[:1024].encode("hex"))
                continue

            # own broadcast from another interface
            try:
                if msgdata['__sid__'] == self.ID:
                    continue
            except KeyError:
                continue

            # message type
            try:
                msg = msgdata['message']
            except KeyError:
                LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
                continue
            # find handler
            try:
                handler = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored silently
                LOG.warning("Unknown message received [%s]" % msg)
                LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue
            # run handler
            try:
                handler(msgdata)
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

    def broadcast_message(self, msg):
        """
        Wysłanie komunikatu do wszystkich odbiorców w sieci
        """
        msg['__sid__'] = self.ID
        msg = self.serializer.serialize(msg, resreq=False)
        self.SOCK.sendto(msg, ('<broadcast>', self.port) )






class UDPBroadcast(UDPLoop):

    def __init__(self, host_id):
        #self.DAEMON = server
        self.ID = host_id
        super(UDPBroadcast, self).__init__()
        self.register_message(messages.WORKER_LIVE, self.handle_remote_worker_join)
        self.register_message(messages.WORKER_LEAVE, self.handle_remote_worker_leave)
        self.register_message(messages.HOST_JOIN, self.handle_host_join)
        self.register_message(messages.HOST_LEAVE, self.handle_host_leave)
        #self.register_message(messages.HOST_REFRESH, self.handle_host_refresh)

    def handle_remote_worker_join(self, msgdata):
        emit("worker-remote-join", msgdata['id'], msgdata['host'], msgdata['addr'], msgdata['service'] )
        #self.DAEMON.WORKER.worker_start(msgdata['id'], msgdata['service'], msgdata['addr'] )

    def handle_remote_worker_leave(self, msgdata):
        emit("worker-remote-leave", msgdata['id'] )
        #self.DAEMON.WORKER.worker_stop(msgdata['id'] )

    def handle_host_join(self, msgdata):
        emit("host-join", msgdata['id'], msgdata['addr'])
        #self.DAEMON.notify_kasayad_start( msgdata['id'], msgdata['hostname'], msgdata['addr'], msgdata['services'])

    def handle_host_leave(self, msgdata):
        emit("host-leave", msgdata['id'] )
        #self.DAEMON.notify_kasayad_stop(msgdata['id'])

    #def handle_host_refresh(self, msgdata):
    #    self.DAEMON.notify_kasayad_refresh(msgdata['id'], services=msgdata['services'])



    # broadcast specific messages

    def broadcast_worker_live(self, host_id, worker_id, address, service):
        """
        Send information to other hosts about running worker
        """
        msg = {
            "message" : messages.WORKER_LIVE,
            "id" : worker_id,
            "host" : host_id,
            "addr" : address,
            "service" : service,
            }
        self.broadcast_message(msg)

    def broadcast_worker_stop(self, worker_id ):
        """
        Send information to other hosts about shutting down worker
        """
        msg = {
            "message" : messages.WORKER_LEAVE,
            "id" : worker_id,
            }
        self.broadcast_message(msg)

    def broadcast_host_start(self, address):
        msg = {
            "message" : messages.HOST_JOIN,
            "id" : self.ID,
            "addr" : address,
            }
        self.broadcast_message(msg)

    def broadcast_host_stop(self):
        msg = {
            "message" : messages.HOST_LEAVE,
            "id" : self.ID,
            }
        self.broadcast_message(msg)

    #def broadcast_host_refresh(self, ID, services=None):
    #    msg = {
    #        "message" : messages.HOST_REFRESH,
    #        "id" : ID,
    #    }
    #    if services:
    #        msg["services"] = services
    #    self.broadcast_message(msg)
