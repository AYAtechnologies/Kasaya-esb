#!/usr/bin/env python
#coding: utf-8
"""

Database which keep information abut all workers nameservers in all network

"""
import random


class DictDB(object):
    """
    Klasa zajmująca się przechowywaniem stanu wszystkich workerów w sieci.
    """

    replaces_broadcast = False

    def __init__(self, server):
        self.SRV = server
        self.services = {}

    def close(self):
        pass

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
