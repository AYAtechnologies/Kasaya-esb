#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from random import choice



class NetworkStateDB(object):

    def __init__(self):
        if settings.KASAYAD_DB_BACKEND=="memory":
            from . import memsqlite
            self.LLDB = memsqlite.MemoryDB()
        else:
            raise Exception("Unknown database backend: %s" % backend)

        self.LLDB.connect()

    def set_own_id(self,id):
        self.own_id = id

    def close(self):
        self.LLDB.close()


    # ---- high level database functions

    # hosts

    def host_register(self, host_id, address):# hostname, ip, services=None):
        """
        Register new host (kasayad instance).
        host_id - id of kasayad
        address - address of remote kasaya daemon
        // hostname - name of host on which is kasayad running
        // ip - ip address of host
        // services - list of services available on host
        """
        # check if host is already registered
        he = self.LLDB.host_get(host_id)
        if not he is None:
            # if current host has different address,
            # then previous was stopped and this is new instance
            # we unregister previous instance before register new one
            #if he['id']!=host_id:
            #    self.host_unregister(host_id)
            #else:
            return False
        # register host
        self.LLDB.host_add(host_id, address)
        # register services
        #if services is not None:
        #    self.service_update_list(host_id, services)
        return True


    def host_unregister(self, host_id):
        """
        Wyrejestrowanie hosta z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        # remove all services for host
        for s in self.LLDB.service_list(host_id):
            self.LLDB.service_del(host_id, s['service'])
        # remove host
        self.LLDB.host_del(host_id)
        #return {"addr":res[2],"hostname":res[1]}


    def host_list(self):
        """
        Return list of all hosts in network
        """
        for h in self.LLDB.host_list():
            yield h


    def host_addr_by_id(self, host_id):
        """
        Return address of host with given host_id
        """
        res = self.LLDB.host_get(host_id)
        if res is None:
            return None
        return res['addr']


    # services


    def service_add(self, host_id, sname):
        """
        Dodanie serwisu do hosta
        """
        self.LLDB.service_add(host_id, sname)

    def service_del(self, host_id, sname):
        """
        Usunięcie serwisu z hosta
        """
        self.LLDB.service_del(host_id, sname)

    def service_clear(self, host_id):
        """
        Usunięcie wszystkich serwisów z serwera
        """
        for s in self.LLDB.service_list():
            self.LLDB.service_del(host_id, s['service'])

    def service_list(self, host_id):
        """
        List of all services available on host
        """
        for s in self.LLDB.service_list(host_id):
            yield s

    def service_update_list(self, host_id, services):
        """
        Aktualizacja listy wbudowanych serwisów
        """
        newset = set(services)
        todel = set()
        for s in self.service_list(host_id):
            name = s['service']
            if name in newset:
                newset.discard(name)
            else:
                todel.add(name)
        changes = 0
        for s in newset:
            self.service_add(host_id, s)
            changes+=1
        for s in todel:
            self.service_del(host_id, s)
            changes+=1
        return changes>0



    # workers

    def worker_register(self, host_id, worker_id, service_name, address, pid = -1, online=True):
        """
        Zarejestrowanie workera w bazie.
          worker_id - ID workera
          host_id - ID hosta do którego worker jest przypisany
          service_name - nazwa serwisu
          addr - adres serwisu

          wynikiem jest:
          True - jeśli nowy worker został zarejestrowany
          False - jesli worker już istnieje w bazie
        """
        # sprawdzenie czy worker istnieje w bazie
        for w in self.LLDB.worker_list(host_id=host_id):
            # is already registered
            if w['id']==worker_id:
                return False
            # another worker under same address, unregister existing
            if w['addr']==address:
                self.worker_unregister(address)
        self.LLDB.worker_add(host_id, worker_id, service_name, address, pid, online)
        return True


    def worker_unregister(self, address=None, ID=None):
        """
        Wyrejestrowanie workera z bazy.
        Parametrem może być adres lub ID workera.
        """
        if address==ID==None:
            raise Exception("Missing parameter (address or ID)")
        if address:
            self.LLDB.worker_del_by_addr(address)
        if ID:
            self.LLDB.worker_del_by_id(ID)


    def worker_set_state(self, worker_id, online):
        """
        Change state of worker to online or offline
        """
        wnfo = self.LLDB.worker_get(worker_id)
        if wnfo is None: return
        if wnfo['online'] == online: return
        self.LLDB.worker_set_state(worker_id, online)


    def worker_get(self, worker_id):
        """
        Return worker details for given ID
        """
        return self.LLDB.worker_get(worker_id)


    def worker_list(self, host_id, only_online=False):
        """
        Return list of workers on host by host ip or host ID
        """
        lst = self.LLDB.worker_list(host_id)
        for wrk in lst:
            yield wrk


    def choose_worker_for_service(self, service):
        lst = self.LLDB.workers_for_service(service, True)
        lst = list(lst)
        if len(lst)==0:
            return None
        return choice(lst)
