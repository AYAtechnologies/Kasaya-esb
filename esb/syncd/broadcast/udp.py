#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

"""

Broadcasting service for synchronising state of all sync servers in network

"""
import settings
from gevent import socket
from esb.protocol import serialize, deserialize


class UDPBroadcast(object):


    def __init__(self, server):
        self.SRV = server
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
            msg = message_decode(msg)
            if msg["message"]=="connect":
                # przyłączenie serwisu
                self.SRV.DB.register(msg['service'], msg['commchannel'])
            elif msg['message']=="disconnect":
                # odłączenie serwisu
                self.SRV.DB.unregister(msg['commchannel'])

            print "Received broadcast >>>",msg, addr


    def broadcast_message(self, msg):
        """
        Wysłanie komunikatu do wszystkich workerów w sieci
        """
        msg = message_encode(msg)
        self.sock.sendto(msg, ('<broadcast>', self.PORT) )
        print "sending broadcast",msg

