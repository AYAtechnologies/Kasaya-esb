#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

#import zerorpc
#import zmq
import json
import sys
from pprint import pprint
import random

import gevent
from gevent_zeromq import zmq
from gevent import socket
#import socket


def message_encode(msg):
    return json.dumps(msg)

def message_decode(msg):
    return json.loads(msg)


class ServiceDB(object):
    """
    Klasa zajmująca się przechowywaniem stanu wszystkich workerów w sieci.
    """

    def __init__(self):
        self.services = {}

    def register(self, name, addr):
        """
        Zarejestrowanie workera w bazie.
          name - nazwa serwisu
          addr - adres ip:port workera
        """
        if not name in self.services:
            self.services[name] = set()
        self.services[name].add(addr)
        print "registered service [%s] addr [%s]" % (name,addr)
        print ">>>",  self.services


    def unregister(self, addr):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli nie znaleziono workera w bazie
        """
        status = False
        #print "pre",self.services
        for i in self.services.values():
            if addr in i:
                i.discard(addr)
                status = True
        print "unregistered server [%s]" % addr
        #print "post",self.services
        #print "status",status
        print "Aktualnie zarejestrowane workery", self.services
        return status
        #BC.send_broadcast("broadcast from ns")


    def get_worker_for_service(self, name):
        """
        Losuje / wybiera workera który realizuje usługę o podanej nazwie
        """
        try:
            servers = self.services[ name ]
        except KeyError:
            return None
        if len(servers)==0:
            return None
        return random.choice( list(servers) )



class NameServer(object):
    """
    Główna klasa nameservera która nasłuchuje na lokalnych socketach i od lokalnych workerów i klientów.
    """

    def __init__(self):
        self.context = zmq.Context()
        # kanał wejściowy ipc dla workerów tylko z tego localhosta
        # tym kanałem workery wysyłają informacje o uruchomieniu i zatrzymaniu
        # Tylko zmiany otrzymane tędy są propagowane przez broadcast do pozostałych nameserwerów.
        self.input = self.context.socket(zmq.PULL)
        self.input.bind('ipc://pingchannel')
        # drugi kanał służy do odpytywania nameserwera przez klientów
        self.queries = self.context.socket(zmq.REP)
        self.queries.bind('ipc://querychannel')

    def run(self):
        """
        Pętla nasłuchująca na lokalnym sockecie o pojawiających się i znikających workerach
        """
        global DB, BC
        while True:
            msg = self.input.recv()
            msg = message_decode(msg)

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
                msg = message_encode(msg)
                self.queries.send(msg)
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                self.queries.send("")




class SocketBroadcast(object):
    """
    Klasa
    """
    def __init__(self):
        self.PORT = 4040
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('',self.PORT))
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    def run(self):
        """
        Pętla odbierająca i rozsyłająca informacje o zmianach stanu workerów w sieci
        """
        while True:
            msg, addr = self.sock.recvfrom(2048)
            msg = message_decode(msg)
            if msg["message"]=="connect":
                # przyłączenie serwisu
                DB.register(msg['service'], msg['commchannel'])
            elif msg['message']=="disconnect":
                # odłączenie serwisu
                DB.unregister(msg['commchannel'])

            print "Received broadcast >>>",msg, addr


    def send_broadcast(self, msg):
        """
        Wysłanie komunikatu do wszystkich workerów w sieci
        """
        msg = message_encode(msg)
        self.sock.sendto(msg, ('<broadcast>', self.PORT) )
        print "sending broadcast",msg


DB = ServiceDB()        # instancja bazy danych
NS = NameServer()       # instancja
BC = SocketBroadcast()  # instancja nasłuchująca na broadcast w sieci

def name_service():
    global NS
    NS.run()

def name_queries():
    global NS
    NS.run_query_loop()

def broadcast_service():
    global BC
    BC.run()



gevent.joinall([
    gevent.spawn(name_service),
    gevent.spawn(name_queries),
    gevent.spawn(broadcast_service),
])



