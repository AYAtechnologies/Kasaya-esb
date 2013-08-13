#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

"""

Broadcasting service for synchronising state of all sync servers in network

"""
from servicebus.conf import settings
from gevent import socket
from servicebus.protocol import serialize, deserialize, messages


class UDPBroadcast(object):


    def __init__(self, server):
        self.SRV = server
        self.port = settings.BROADCAST_PORT
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('',settings.BROADCAST_PORT))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)


    def close(self):
        #self.sock.shutdown()
        self.sock.close()


    def run_listener(self):
        """
        Pętla odbierająca i rozsyłająca informacje o zmianach stanu workerów w sieci
        """
        while True:
            msg, addr = self.sock.recvfrom(2048)
            msg = deserialize(msg)

            if msg['message'] in (
                messages.WORKER_JOIN,
                messages.WORKER_LEAVE):
                self.SRV.WORKER.worker_change_state(msg, frombroadcast=True)

            print "Received broadcast >>>",msg, addr


    def broadcast_message(self, msg):
        """
        Wysłanie komunikatu do wszystkich workerów w sieci
        """
        print "sending broadcast", msg
        msg = serialize(msg)
        self.sock.sendto(msg, ('<broadcast>', self.port) )

