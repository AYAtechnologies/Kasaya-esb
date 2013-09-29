#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings



class NetworkStateDB(object):

    replaces_broadcast = False

    def __init__(self):
        if settings.SYNC_DB_BACKEND=="memory":
            from . import memsqlite
            self.LLDB = memsqlite.MemoryDB()
        else:
            raise Exception("Unknown database backend: %s" % backend)

        self.LLDB.connect()
        #self.LLDB.add_collection("hosts")
        #self.LLDB.add_collection("workers")
        #self.LLDB.add_collection("services")

    def set_own_ip(self,ip):
        self.LLDB.set_own_ip(ip)

    def close(self):
        self.LLDB.close()


    # ---- high level database functions

    # hosts

    def host_register(self, host_uuid, hostname, addr, services=None):
        # check if host is already registered
        he = self.LLDB.host_exist(host_uuid=host_uuid, addr=addr)
        if he is not None:
            # if current host has different UUID,
            # then previous was stopped and this is new instance
            # we unregister previous instance before register new one
            if he['uuid']!=host_uuid:
                self.host_unregister(host_uuid)
        # register host
        self.LLDB.host_add(host_uuid, addr, hostname)
        # register services
        self.service_update_list(host_uuid, services)



    def host_unregister(self, host_uuid):
        """
        Wyrejestrowanie hosta z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        # remove all services for host
        for s in self.LLDB.service_list(host_uuid):
            self.LLDB.service_del(host_uuid, s)
        # remove host
        self.LLDB.host_del(host_uuid)
        #return {"addr":res[2],"hostname":res[1]}


    # services



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
        for w in self.LLDB.worker_list(ip):
            if w['uuid']==worker_uuid:
                # already registered
                return False
            if (w['ip']==ip) and (w['port']==port):
                # another worker under same address, unregister existing
                self.worker_unregister(ip, port)

        self.LLDB.worker_add(worker_uuid, service_name, ip, port, pid)
        return True


    def worker_unregister(self, ip,port):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        self.LLDB.worker_del(ip,port)



    # ----------------

    def worker_list_local(self):
        for w in self.LLDB.worker_list_local():
            yield w



    def get_workers_for_service(self, service_name):
        """
        Wszystkie workery udostępniające podany serwis
        """
        self.cur.execute(
            "SELECT ip,port FROM workers WHERE service=?",
            [service_name] )
        lst = self.cur.fetchall()
        return lst



    # global network state

    def host_add_service(self, host_uuid, sname):
        """
        Dodanie serwisu do hosta
        """
        self.cur.execute(
            "INSERT INTO services ('host_uuid','name') VALUES (?,?)",
            (host_uuid, sname))

    def host_del_service(self, host_uuid, sname):
        """
        Usunięcie serwisu z hosta
        """
        self.cur.execute(
            "DELETE FROM services WHERE host_uuid=? AND name=?",
            (host_uuid, sname) )

    def host_clear_services(self, host_uuid):
        """
        Usunięcie wszystkich serwisów z serwera
        """
        self.cur.execute(
            "DELETE FROM services WHERE host_uuid=?",
            [host_uuid] )



    def host_services(self, uuid):
        """
        List of all services available on host
        """
        self.cur.execute( "SELECT name FROM services WHERE host_uuid=?", (uuid,) )
        lst = self.cur.fetchall()
        res = [ l[0] for l in lst ]
        return res



    def service_update_list(self, host_uuid, services):
        pass


#    def host_update_services(self, host_uuid, services):
#        """
#        Aktualizacja listy wbudowanych serwisów
#        """
#        newset = set(services)
#        todel = set()
#        for s in self.host_services(host_uuid):
#            if s in newset:
#                newset.discard(s)
#            else:
#                todel.add(s)
#        for s in newset:
#            self.host_add_service(host_uuid, s)
#        for s in todel:
#            self.host_del_service(host_uuid, s)


    # raportowanie stanu sieci


    def host_list(self):
        """
        Return list of all hosts in network
        """
        self.cur.execute( "SELECT uuid, addr, hostname FROM hosts ORDER BY addr" )
        lst = self.cur.fetchall()
        return lst


    def workers_on_host(self, host_uuid):
        addr = self.host_addr_by_uuid(host_uuid)
        if addr==None:
            return []
        self.cur.execute( "SELECT uuid, service, ip, port, pid FROM workers WHERE ip=?", (addr,) )
        lst = self.cur.fetchall()
        return lst


    def host_addr_by_uuid(self, uuid):
        """
        Zwraca adres IP na którym znajduje się host o podanym UUID
        """
        self.cur.execute( "SELECT addr FROM hosts WHERE uuid=?", (uuid,) )
        host = self.cur.fetchone()
        if not host is None:
            return host[0]

    def worker_ip_port_by_uuid(self, uuid, pid=False):
        """
        zwraca ip i port workera o podanym uuid
        """
        self.cur.execute( "SELECT ip,port,pid FROM workers WHERE uuid=?", (uuid,) )
        wrk = self.cur.fetchone()
        if wrk is None:
            return None, None
        else:
            if pid:
                return wrk[0], wrk[1], wrk[2]
            return wrk[0], wrk[1]
