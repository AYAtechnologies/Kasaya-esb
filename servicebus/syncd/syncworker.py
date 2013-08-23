#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.conf import settings
from servicebus import exceptions
import gevent
from datetime import datetime, timedelta
from gevent_zeromq import zmq
from gevent import socket
from servicebus.protocol import serialize, deserialize, messages
from servicebus.binder import get_bind_address

from pprint import pprint
import random
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
        # kanał dialogowy pomiędzy serwerami syncd
        print get_bind_address(settings.SYNCD_CONTROL_BIND)


    def close(self):
        self.local_input.close()
        self.queries.close()

    def worker_start(self, service, address, local):
        self.SRV.DB.worker_register(service, address, local)
        if local:
            self.SRV.BC.send_worker_start(service, address)

    def worker_stop(self, address, local):
        self.SRV.DB.worker_unregister(address)
        if local:
            self.SRV.BC.send_worker_stop(address)


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

            # join network
            if msg==messages.WORKER_JOIN:
                self.worker_start(msgdata['service'], msgdata['addr'], True )
            elif msg==messages.WORKER_LEAVE:
                self.worker_stop( msgdata['addr'], True )


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
                result = {
                    'message':messages.WORKER_ADDR,
                    'service':name,
                    'addr':res
                }

            elif msg==messages.CTL_CALL:
                print "CONTROL REQUEST"
                print msgdata
                result = {
                    "message":messages.NOOP
                }

            else:
                result = {
                    "message":messages.NOOP
                }
            self.queries.send( serialize(result) )


    def run_hearbeat_loop(self):
        pinglife = timedelta( seconds = settings.HEARTBEAT_TIMEOUT )

        while True:
            msg = {"message":messages.PING}
            lworkers = self.SRV.DB.get_local_workers()
            for worker in lworkers:
                now = datetime.now()

                # is last heartbeat fresh enough?
                lhb = self.SRV.DB.get_last_worker_heartbeat(worker)
                delta = now-lhb
                if delta<=pinglife:
                    continue

                # ping with timeout
                try:
                    with gevent.Timeout(settings.PING_TIMEOUT, TooLong):
                        pingres = self.ping_worker(worker)
                    if pingres:
                        self.SRV.DB.set_last_worker_heartbeat(worker, now)
                        continue
                except TooLong:
                    self.PINGER.close()
                    self.PINGER = None

                # worker died
                print "worker", worker, "died or broken"
                self.SRV.DB.unregister(worker)
                msg = {"message":messages.WORKER_LEAVE, "addr":worker}
                self.SRV.BC.broadcast_message(msg)

            gevent.sleep(settings.WORKER_HEARTBEAT)


    def ping_worker(self, addr):
        #context = zmq.Context()
        self.PINGER = self.context.socket(zmq.REQ)
        self.PINGER.connect(addr)
        # send ping
        msg = {"message":messages.PING}
        self.PINGER.send( serialize(msg) )
        # result of ping
        try:
            res = self.PINGER.recv()
            self.PINGER.close()
            res = deserialize(res)
            if res["message"] != messages.PONG:
                return False
        except:
            return False
        return True


    def request_workers_register(self):
        """
        Send to all local workers request for registering in network.
        Its used after new host start.
        """
        msg = {"message":messages.WORKER_REREG}
        for worker in self.SRV.DB.get_local_workers():
            sck = self.context.socket(zmq.REQ)
            try:
                sck.connect(worker)
                sck.send( serialize(msg) )
                res = sck.recv()
            finally:
                sck.close()


