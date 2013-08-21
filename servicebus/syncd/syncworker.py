#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

"""

Core service of nameserver

"""
from servicebus.conf import settings
from servicebus import exceptions
import gevent
from datetime import datetime, timedelta
from gevent_zeromq import zmq
from gevent import socket
from servicebus.protocol import serialize, deserialize, messages

from pprint import pprint
import sys


class TooLong(Exception): pass


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
            self.SRV.DB.register(msg['service'], msg['addr'], msg['local'])
            msg['local'] = False
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
            try:
                msgdata = deserialize(msgdata)
            except exceptions.ServiceBusException:
                continue

            msg = msgdata['message']

            # join / leave net by network
            if msg in (
                messages.WORKER_JOIN,
                messages.WORKER_LEAVE):
                try:
                    self.worker_change_state(msgdata)
                except:
                    print "State change fail"
                    pass


    def run_query_loop(self):
        """
        Pętla nasłuchująca na lokalnym sockecie i odpowiadająca klientom na zapytania o workery
        """
        while True:
            msgdata = self.queries.recv()
            try:
                msgdata = deserialize(msgdata)
            except exceptions.ServiceBusException:
                self.queries.send("")
                continue

            msg = msgdata['message']

            if msg==messages.QUERY:
                # pytanie o worker
                name = msgdata['service']
                res = self.SRV.DB.get_worker_for_service(name)
                reply = {'message':messages.WORKER_ADDR, 'service':name, 'addr':res}
                self.queries.send( serialize(reply) )
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                self.queries.send("")


    def run_hearbeat_loop(self):
        pinglife = timedelta( seconds = settings.HEARTBEAT_TIMEOUT )

        while True:
            msg = {"message":messages.PING}
            lworkers = self.SRV.DB.get_local_workers()
            for worker in lworkers:
                now = datetime.now()

                # is last heartbeat fresh enough?
                lhb = self.SRV.DB.get_last_heartbeat(worker)
                delta = now-lhb
                if delta<=pinglife:
                    continue

                # ping with timeout
                try:
                    with gevent.Timeout(settings.PING_TIMEOUT, TooLong):
                        pingres = self.ping_worker(worker)
                    if pingres:
                        self.SRV.DB.set_last_heartbeat(worker, now)
                        continue
                except TooLong:
                    pass

                # worker died
                print "worker", worker, "died or broken"
                self.SRV.DB.unregister(worker)
                msg = {"message":messages.WORKER_LEAVE, "addr":worker}
                self.SRV.BC.broadcast_message(msg)

            gevent.sleep(settings.WORKER_HEARTBEAT)


    def ping_worker(self, addr):
        context = zmq.Context()
        PINGER = context.socket(zmq.REQ)
        PINGER.connect(addr)
        # send ping
        msg = {"message":messages.PING}
        PINGER.send( serialize(msg) )
        # result of ping
        try:
            res = PINGER.recv()
            res = deserialize(res)
            if res["message"] != messages.PONG:
                return False
        except:
            return False
        return True



