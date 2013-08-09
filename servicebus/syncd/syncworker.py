#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

"""

Core service of nameserver

"""
import settings
import gevent
from gevent_zeromq import zmq
from gevent import socket
from servicebus.protocol import serialize, deserialize

from pprint import pprint
import sys


class SyncWorker(object):
    """
    Główna klasa nameservera która nasłuchuje na lokalnych socketach i od lokalnych workerów i klientów.
    """

    def __init__(self, server):
        self.SRV = server
        self.context = zmq.Context()
        # kanał wejściowy ipc dla workerów tylko z tego localhosta
        # tym kanałem workery wysyłają informacje o uruchomieniu i zatrzymaniu
        # Tylko zmiany otrzymane tędy są propagowane przez broadcast do pozostałych nameserwerów.
        self.local_input = self.context.socket(zmq.PULL)
        self.local_input.bind('ipc://'+settings.SOCK_LOCALWORKERS)
        # drugi kanał służy do odpytywania nameserwera przez klientów
        self.queries = self.context.socket(zmq.REP)
        self.queries.bind('ipc://'+settings.SOCK_QUERIES)


    def close(self):
        self.local_input.close()
        self.queries.close()


    def run_local_loop(self):
        """
        Pętla nasłuchująca na lokalnym sockecie o pojawiających się i znikających workerach na własnym localhoście
        """
        global DB, BC
        while True:
            msg = self.local_input.recv()
            msg = deserialize(msg)

            if msg['message']=="connect":
                # przyłączenie serwisu
                DB.register(msg['service'], msg['commchannel'])
                BC.send_broadcast(msg)
            elif msg['message']=="disconnect":
                # odłączenie serwisu
                DB.unregister(msg['commchannel'])
                BC.send_broadcast(msg)
            else:
                pprint(msg)

            sys.stdout.flush()


    def run_query_loop(self):
        """
        Pętla nasłuchująca na lokalnym sockecie i odpowiadająca klientom na zapytania o workery
        """
        global DB
        while True:
            msg = self.queries.recv()
            msg = message_decode(msg)

            if msg['message']=="query":
                name = msg['service']
                print "pytanie o worker", name
                res = DB.get_worker_for_service(name)
                msg = {'message':'result', 'service':name, 'address':res}
                msg = serialize(msg)
                self.queries.send(msg)
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                self.queries.send("")


