#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings




class NetworkStateDB(object):

    def __init__(self):
        if settings.SYNC_DB_BACKEND=="memory":
            from . import memsqlite
            self.DB = memsqlite.MemoryDB()
        else:
            raise Exception("Unknown database backend: %s" % backend)

        self.DB.connect()
        #self.DB.add_collection("hosts")
        #self.DB.add_collection("workers")
        #self.DB.add_collection("services")

    def close(self):
        self.DB.close()


    # ---- high level database functions



    def worker_register(self, worker_uuid, service_name, ip,port, pid, localservice):
        """
        Zarejestrowanie workera w bazie.
          name - nazwa serwisu
          addr - adres ip:port workera
          localservice - worker jest na lokalnym hoście

          wynikiem jest:
          True - jeśli nowy worker został zarejestrowany
          False - jesli worker już istnieje w bazie

          jeśli worker istnieje, ale nie zgadzają się dane z istniejącymi w bazie
          należy rzucić wyjątek DBException
        """
        # sprawdzenie czy worker istnieje w bazie
        self.cur.execute(
            "SELECT * FROM workers WHERE uuid=?",
            [worker_uuid] )
        res = self.cur.fetchone()
        # worker już jest zarejestrowany
        if not res is None:
            #uu, sv, ad, lc = res
            if (res[0]==worker_uuid):
                return False
        self.worker_unregister(ip,port)
        # dodanie nowego workera do bazy
        self.cur.execute(
            "INSERT INTO workers ('uuid','ip','port', 'service','pid', 'local') VALUES (?,?,?,?,?,?)",
            (worker_uuid, ip,port, service_name, pid, localservice))
        self.__db.commit()
        return True


    def worker_unregister(self, ip,port):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        self.cur.execute(
            "SELECT * FROM workers WHERE ip=? AND port=?",
            [ip,port] )
        res = self.cur.fetchone()
        # worker nie istnieje
        if res==None:
            return False
        # wykasowanie
        self.cur.execute(
            "DELETE FROM workers WHERE ip=? AND port=?",
            [ip,port] )
        self.__db.commit()
        return True


    def get_workers_for_service(self, service_name):
        """
        Wszystkie workery udostępniające podany serwis
        """
        self.cur.execute(
            "SELECT ip,port FROM workers WHERE service=?",
            [service_name] )
        lst = self.cur.fetchall()
        return lst


    def get_local_workers(self):
        """
        Return list of live workers on local host
        """
        self.cur.execute(
            "SELECT uuid,service,ip,port,pid FROM workers WHERE local=?",
            [True] )
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


    def host_register(self, host_uuid, hostname, addr, services=None):
        # sprawdzenie czy worker istnieje w bazie
        self.cur.execute(
            "SELECT * FROM hosts WHERE uuid=?",
            [host_uuid] )
        res = self.cur.fetchone()
        # host już jest zarejestrowany
        if not res is None:
            #uu, sv, ad, lc = res
            if (res[0]==host_uuid):
                return False
        # dodanie nowego hosta do bazy
        self.cur.execute(
            "INSERT INTO hosts ('uuid','addr','hostname') VALUES (?,?,?)",
            (host_uuid, addr, hostname))
        self.__db.commit()
        # services available on host
        if not services is None:
            self.host_update_services(host_uuid, services)
        return True

    def host_unregister(self, uuid):
        """
        Wyrejestrowanie hosta z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        self.cur.execute(
            "SELECT * FROM hosts WHERE uuid=?",
            [uuid] )
        res = self.cur.fetchone()

        # host nie istnieje
        if res==None:
            return False

        # wykasowanie istniejącego
        self.cur.execute(
            "DELETE FROM hosts WHERE uuid=?",
            [uuid] )
        # usunięcie wpisów o serwisach
        self.host_clear_services(uuid)
        self.__db.commit()

        return {"addr":res[2],"hostname":res[1]}


    def host_services(self, uuid):
        """
        List of all services available on host
        """
        self.cur.execute( "SELECT name FROM services WHERE host_uuid=?", (uuid,) )
        lst = self.cur.fetchall()
        res = [ l[0] for l in lst ]
        return res


    def host_update_services(self, host_uuid, services):
        """
        Aktualizacja listy wbudowanych serwisów
        """
        newset = set(services)
        todel = set()
        for s in self.host_services(host_uuid):
            if s in newset:
                newset.discard(s)
            else:
                todel.add(s)
        for s in newset:
            self.host_add_service(host_uuid, s)
        for s in todel:
            self.host_del_service(host_uuid, s)


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
