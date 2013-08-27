#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

"""

Broadcasting service for synchronising state of all sync servers in network

"""
from servicebus.conf import settings
from gevent import socket
from gevent_zeromq import zmq
from servicebus.protocol import serialize, deserialize, messages


class UDPBroadcast(object):


    def __init__(self, server):
        self.SRV = server
        # broadcast send/receive
        self.port = settings.BROADCAST_PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('',settings.BROADCAST_PORT))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        ## syncd dialog
        #self.sync = socket
        #self.queries = self.context.socket(zmq.REP)
        #self.queries.bind('ipc://'+settings.SOCK_QUERIES)


    def close(self):
        #self.sock.shutdown()
        self.sock.close()


    def run_listener(self):
        """
        Pętla odbierająca i rozsyłająca informacje o zmianach stanu workerów w sieci
        """
        while True:
            msgdata, addr = self.sock.recvfrom(4096)
            try:
                msgdata = deserialize(msgdata)
                msg = msgdata['message']
            except:
                continue
            #print "incoming broadcast", msgdata

            if msg==messages.WORKER_JOIN:
                self.SRV.WORKER.worker_start(msgdata['service'], msgdata['addr'], False )

            elif msg==messages.WORKER_LEAVE:
                self.SRV.WORKER.worker_stop(msgdata['addr'], False )

            elif msg== messages.HOST_JOIN:
                self.SRV.notify_syncd_start(msgdata['uuid'], msgdata['hostname'], msgdata['addr'])

            elif msg== messages.HOST_LEAVE:
                self.SRV.notify_syncd_stop(msgdata['uuid'])


    def broadcast_message(self, msg):
        """
        Wysłanie komunikatu do wszystkich workerów w sieci
        """
        #print "sending broadcast", msg
        msg = serialize(msg)
        self.sock.sendto(msg, ('<broadcast>', self.port) )


    # broadcast specific messages

    def send_worker_start(self, service, address):
        """
        Send information to other hosts about new worker
        """
        msg = {
            "message" : messages.WORKER_JOIN,
            "addr" : address,
            "service" : service,
            }
        self.broadcast_message(msg)

    def send_worker_stop(self, address):
        """
        Send information to other hosts about shutting down worker
        """
        msg = {
            "message" : messages.WORKER_LEAVE,
            "addr" : address,
            }
        self.broadcast_message(msg)


    def send_host_start(self, uuid, hostname, address=None):
        msg = {
            "message" : messages.HOST_JOIN,
            "hostname" : hostname,
            "addr" : address,
            "uuid" : uuid
            }
        self.broadcast_message(msg)

    def send_host_stop(self, uuid):
        msg = {
            "message" : messages.HOST_LEAVE,
            "uuid" : uuid
            }
        self.broadcast_message(msg)
