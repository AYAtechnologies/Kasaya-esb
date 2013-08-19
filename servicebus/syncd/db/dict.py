#!/usr/bin/env python
#coding: utf-8
"""

Database which keep information abut all workers nameservers in all network

"""
import random
from datetime import datetime


class DictDB(object):
    """
    Klasa zajmująca się przechowywaniem stanu wszystkich workerów w sieci.
    """

    replaces_broadcast = False

    def __init__(self, server):
        self.SRV = server
        self.services = {}
        self.local_workers = {}


    def close(self):
        pass


    def register(self, name, addr, localservice):
        """
        Zarejestrowanie workera w bazie.
          name - nazwa serwisu
          addr - adres ip:port workera
          localservice - worker jest na lokalnym hoście
        """
        # nowy serwis w sieci
        if not name in self.services:
            self.services[name] = []
        # nowy worker dla serwisu
        if not addr in self.services[name]:
            self.services[name].append(addr)
        # worker na localhoście
        if localservice:
            self.set_last_heartbeat(addr)


    def unregister(self, addr):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli nie znaleziono workera w bazie
        """
        status = False
        for i in self.services.values():
            if addr in i:
                i.remove(addr)
                status = True
        if addr in self.local_workers:
            del self.local_workers[addr]
        return status


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
        return random.choice( servers )


    def get_local_workers(self):
        return self.local_workers.keys()


    def set_last_heartbeat(self, addr, htime=None):
        if htime is None:
            htime = datetime.now()
        self.local_workers[addr] = htime


    def get_last_heartbeat(self, addr):
        try:
            return self.local_workers[addr]
        except AttributeError:
            return None
