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
        self.hosts = {}
        self.local_workers = {}


    def close(self):
        pass


    def worker_register(self, name, addr, localservice):
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
            return True
        return False


    def worker_unregister(self, addr):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        status = False
        for i in self.services.values():
            if addr in i:
                i.remove(addr)
                status = True
        if addr in self.local_workers:
            del self.local_workers[addr]
            status = True
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
        """
        Return list of live workers on local host
        """
        return self.local_workers.keys()


    # global network state

    def host_register(self, uuid, hostname, addr):
        if uuid in self.hosts:
            return False
        self.hosts[uuid] = {
            'hostname' : hostname,
            'addr' : addr,
        }
        return True

    def host_unregister(self, uuid, hostname, addr):
        if not uuid in self.hosts:
            return False
        del self.hosts[uuid]
        return True

