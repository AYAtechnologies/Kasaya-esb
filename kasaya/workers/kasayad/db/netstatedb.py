#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from random import choice



class NetworkStateDB(object):

    replaces_broadcast = False

    def __init__(self):
        if settings.KASAYAD_DB_BACKEND=="memory":
            from . import memsqlite
            self.LLDB = memsqlite.MemoryDB()
        else:
            raise Exception("Unknown database backend: %s" % backend)

        self.LLDB.connect()

    def set_own_ip(self,ip):
        self.own_ip = ip

    def close(self):
        self.LLDB.close()


    # ---- high level database functions

    # hosts

    def host_register(self, host_uuid, hostname, ip, services=None):
        # check if host is already registered
        he = self.LLDB.host_exist(host_uuid=host_uuid, ip=ip)
        if not he is None:
            # if current host has different UUID,
            # then previous was stopped and this is new instance
            # we unregister previous instance before register new one
            if he['uuid']!=host_uuid:
                self.host_unregister(host_uuid)
            else:
                return False
        # register host
        self.LLDB.host_add(host_uuid, ip, hostname)
        # register services
        self.service_update_list(host_uuid, services)
        return True


    def host_unregister(self, host_uuid):
        """
        Wyrejestrowanie hosta z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        # remove all services for host
        for s in self.LLDB.service_list(host_uuid):
            self.LLDB.service_del(host_uuid, s['service'])
        # remove host
        self.LLDB.host_del(host_uuid)
        #return {"addr":res[2],"hostname":res[1]}


    def host_list(self):
        """
        Return list of all hosts in network
        """
        for h in self.LLDB.host_list():
            yield h


    def host_addr_by_uuid(self, uuid):
        """
        Zwraca adres IP na którym znajduje się host o podanym UUID
        """
        res = self.LLDB.host_get(uuid)
        if res is None:
            return None
        return res['ip']


    # services


    def service_add(self, host_uuid, sname):
        """
        Dodanie serwisu do hosta
        """
        self.LLDB.service_add(host_uuid, sname)

    def service_del(self, host_uuid, sname):
        """
        Usunięcie serwisu z hosta
        """
        self.LLDB.service_del(host_uuid, sname)

    def service_clear(self, host_uuid):
        """
        Usunięcie wszystkich serwisów z serwera
        """
        for s in self.LLDB.service_list():
            self.LLDB.service_del(host_uuid, s['service'])

    def service_list(self, uuid):
        """
        List of all services available on host
        """
        for s in self.LLDB.service_list(uuid):
            yield s

    def service_update_list(self, host_uuid, services):
        """
        Aktualizacja listy wbudowanych serwisów
        """
        newset = set(services)
        todel = set()
        for s in self.service_list(host_uuid):
            name = s['service']
            if name in newset:
                newset.discard(name)
            else:
                todel.add(name)
        changes = 0
        for s in newset:
            self.service_add(host_uuid, s)
            changes+=1
        for s in todel:
            self.service_del(host_uuid, s)
            changes+=1
        return changes>0



    # workers

    def worker_register(self, worker_uuid, service_name, ip, port, pid):
        """
        Zarejestrowanie workera w bazie.
          worker_uuid - uuid workera
          service_name - nazwa serwisu
          ip, port - adres ip:port workera

          wynikiem jest:
          True - jeśli nowy worker został zarejestrowany
          False - jesli worker już istnieje w bazie
        """
        # sprawdzenie czy worker istnieje w bazie
        for w in self.LLDB.worker_list(ip=ip):
            if w['uuid']==worker_uuid:
                # already registered
                return False
            if (w['ip']==ip) and (w['port']==port):
                # another worker under same address, unregister existing
                self.worker_unregister(ip, port)

        self.LLDB.worker_add(worker_uuid, service_name, ip, port, pid)
        return True


    def worker_unregister(self, ip, port):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        self.LLDB.worker_del(ip,port)


    def worker_get(self, worker_uuid):
        return self.LLDB.worker_get(worker_uuid)


    def worker_list(self, host_uuid=None, ip=None):
        """
        Return list of workers on host by host ip or host uuid
        """
        if host_uuid==ip==None:
            raise Exception("Missing parameter uuid or ip")
        if ip is None:
            ip = self.host_addr_by_uuid(host_uuid)
            if ip is None:
                return

        lst = self.LLDB.worker_list(ip)
        if lst is None:
            return
        for wrk in lst:
            yield wrk


    def worker_list_local(self):
        for w in self.LLDB.worker_list(ip=self.own_ip):
            yield w


    def choose_worker_for_service(self, service):
        lst = self.LLDB.worker_list_local_services(self.own_ip, service)
        if len(lst)==0:
            return None
        return choice(lst)
