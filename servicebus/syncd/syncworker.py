#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

"""

Core service of nameserver

"""
from servicebus.conf import settings
import gevent
from gevent_zeromq import zmq
from gevent import socket
from servicebus.protocol import serialize, deserialize, messages

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


    def worker_change_state(self, msg, frombroadcast=False):
        """
        Register and broadcast worker state change
        """
        if msg['message'] == messages.WORKER_JOIN:
            # przyłączenie serwisu
            self.SRV.DB.register(msg['service'], msg['addr'])
        elif msg['message'] == messages.WORKER_LEAVE:
            # odłączenie serwisu
            self.SRV.DB.unregister(msg['addr'])
        # rozesłanie w sieci
        if not frombroadcast:
            self.SRV.BC.broadcast_message(msg)


    def run_local_loop(self):
        """
        Pętla nasłuchująca na lokalnym sockecie o pojawiających się i znikających workerach na własnym localhoście
        """
        while True:
            msgdata = self.local_input.recv()
            msgdata = deserialize(msgdata)
            msg = msgdata['message']

            # join / leave net by network
            if msg in (
                messages.WORKER_JOIN,
                messages.WORKER_LEAVE):
                self.worker_change_state(msgdata)

            print
            pprint(msgdata)
            sys.stdout.flush()


    def run_query_loop(self):
        """
        Pętla nasłuchująca na lokalnym sockecie i odpowiadająca klientom na zapytania o workery
        """
        while True:
            msgdata = self.queries.recv()
            msgdata = deserialize(msgdata)
            msg = msgdata['message']

            if msg==messages.QUERY:
                # pytanie o worker
                name = msgdata['service']
                print "pytanie o worker", name
                res = self.SRV.DB.get_worker_for_service(name)
                reply = {'message':messages.WORKER_ADDR, 'service':name, 'addr':res}
                self.queries.send( serialize(reply) )
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                self.queries.send("")


