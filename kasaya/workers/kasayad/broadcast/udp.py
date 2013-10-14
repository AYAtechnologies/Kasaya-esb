#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals
from kasaya.conf import settings
from gevent import socket
from kasaya.core.protocol import serialize, deserialize, messages
from kasaya.core.lib import LOG
from kasaya.core.exceptions import NotOurMessage
import traceback


class UDPLoop(object):
    """
    PullLoop is receiving only loop for messages.
    """

    def __init__(self):
        self.is_running = True
        self.own_ip = None
        self.uuid = None
        self._msgdb = {}
        self.port = settings.BROADCAST_PORT
        self.SOCK = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.SOCK.bind(('',settings.BROADCAST_PORT))
        self.SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

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
                msgdata = deserialize(msgdata)
            except NotOurMessage:
                continue
            except Exception:
                LOG.warning("Message from broadcast deserialisation error")
                LOG.debug("Broken message body dump in hex (only first 1024 bytes):\n%s" % msgdata[:1024].encode("hex"))
                continue

            # own broadcast from another interface
            try:
                if msgdata['suuid'] == self.uuid:
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




class UDPBroadcast(UDPLoop):

    def __init__(self, server):
        self.DAEMON = server
        self.uuid = server.uuid
        super(UDPBroadcast, self).__init__()
        self.register_message(messages.WORKER_LIVE, self.handle_worker_join)
        self.register_message(messages.WORKER_LEAVE, self.handle_worker_leave)
        self.register_message(messages.HOST_JOIN, self.handle_host_join)
        self.register_message(messages.HOST_LEAVE, self.handle_host_leave)
        self.register_message(messages.HOST_REFRESH, self.handle_host_refresh)

    def handle_worker_join(self, msgdata):
        self.DAEMON.WORKER.worker_start(msgdata['uuid'], msgdata['service'], msgdata['ip'], msgdata['port'] )

    def handle_worker_leave(self, msgdata):
        self.DAEMON.WORKER.worker_stop(msgdata['ip'], msgdata['port'] )

    def handle_host_join(self, msgdata):
        self.DAEMON.notify_kasayad_start( msgdata['uuid'], msgdata['hostname'], msgdata['addr'], msgdata['services'])

    def handle_host_leave(self, msgdata):
        self.DAEMON.notify_kasayad_stop(msgdata['uuid'])

    def handle_host_refresh(self, msgdata):
        self.DAEMON.notify_kasayad_refresh(msgdata['uuid'], services=msgdata['services'])


    # sending broadcast


    def broadcast_message(self, msg):
        """
        Wysłanie komunikatu do wszystkich workerów w sieci
        """
        msg['suuid'] = self.uuid
        msg = serialize(msg)
        self.SOCK.sendto(msg, ('<broadcast>', self.port) )


    # broadcast specific messages

    def send_worker_live(self, uuid, service, ip,port):
        """
        Send information to other hosts about running worker
        """
        msg = {
            "message" : messages.WORKER_LIVE,
            "uuid" : uuid,
            "ip" : ip,
            "port": port,
            "service" : service#,
            #"pid": pid
            }
        self.broadcast_message(msg)

    def send_worker_stop(self, ip,port):
        """
        Send information to other hosts about shutting down worker
        """
        msg = {
            "message" : messages.WORKER_LEAVE,
            "ip" : ip,
            "port": port,
            }
        self.broadcast_message(msg)

    def send_host_start(self, uuid, hostname, address=None, services=None):
        msg = {
            "message" : messages.HOST_JOIN,
            "hostname" : hostname,
            "addr" : address,
            "uuid" : uuid,
            "services" : services,
            }
        self.broadcast_message(msg)

    def send_host_stop(self, uuid):
        msg = {
            "message" : messages.HOST_LEAVE,
            "uuid" : uuid
            }
        self.broadcast_message(msg)

    def send_host_refresh(self, uuid, services=None):
        msg = {
            "message" : messages.HOST_REFRESH,
            "uuid" : uuid
        }
        if services:
            msg["services"] = services
        self.broadcast_message(msg)
