#!/usr/bin/env python
#coding: utf-8
from datetime import datetime
from base import BaseDB, DBException
import sqlite3 as SQ
from gevent.coros import Semaphore



class MemoryDB(BaseDB):
    """
    Klasa zajmująca się przechowywaniem stanu wszystkich workerów w sieci.
    """

    def __init__(self):
        self.__db = SQ.connect(":memory:")
        self.cur=self.__db.cursor()
        self.cur.execute("CREATE TABLE hosts (uuid TEXT, hostname TEXT, addr TEXT)")
        self.cur.execute("CREATE TABLE workers (uuid TEXT, service TEXT, ip TEXT, port INT, pid INT, local INT)")
        self.SEMA = Semaphore()

    def close(self):
        self.__db.close()



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
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT * FROM workers WHERE uuid=?",
            [worker_uuid] )
        res = self.cur.fetchone()
        self.SEMA.release()
        # worker już jest zarejestrowany
        if not res is None:
            #uu, sv, ad, lc = res
            if (res[0]==worker_uuid):
                return False
        self.worker_unregister(ip,port)
        # dodanie nowego workera do bazy
        self.SEMA.acquire()
        self.cur.execute(
            "INSERT INTO workers ('uuid','ip','port', 'service','pid', 'local') VALUES (?,?,?,?,?,?)",
            (worker_uuid, ip,port, service_name, pid, localservice))
        self.__db.commit()
        self.SEMA.release()
        return True


    def worker_unregister(self, ip,port):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT * FROM workers WHERE ip=? AND port=?",
            [ip,port] )
        res = self.cur.fetchone()
        self.SEMA.release()
        # worker nie istnieje
        if res==None:
            return False
        # wykasowanie
        self.SEMA.acquire()
        self.cur.execute(
            "DELETE FROM workers WHERE ip=? AND port=?",
            [ip,port] )
        self.__db.commit()
        self.SEMA.release()
        return True


    def get_workers_for_service(self, service_name):
        """
        Wszystkie workery udostępniające podany serwis
        """
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT ip,port FROM workers WHERE service=?",
            [service_name] )
        lst = self.cur.fetchall()
        self.SEMA.release()
        return lst


    def get_local_workers(self):
        """
        Return list of live workers on local host
        """
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT uuid,service,ip,port FROM workers WHERE local=?",
            [True] )
        lst = self.cur.fetchall()
        self.SEMA.release()
        return lst


    # global network state

    def host_register(self, host_uuid, hostname, addr):
        # sprawdzenie czy worker istnieje w bazie
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT * FROM hosts WHERE uuid=?",
            [host_uuid] )
        res = self.cur.fetchone()
        self.SEMA.release()
        # host już jest zarejestrowany
        if not res is None:
            #uu, sv, ad, lc = res
            if (res[0]==host_uuid):
                return False
        # dodanie nowego hosta do bazy
        self.SEMA.acquire()
        self.cur.execute(
            "INSERT INTO hosts ('uuid','addr','hostname') VALUES (?,?,?)",
            (host_uuid, addr, hostname))
        self.__db.commit()
        self.SEMA.release()
        return True


    def host_unregister(self, uuid):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT * FROM hosts WHERE uuid=?",
            [uuid] )
        res = self.cur.fetchone()
        self.SEMA.release()
        # host nie istnieje
        if res==None:
            return False
        # wykasowanie istniejącego
        self.SEMA.acquire()
        self.cur.execute(
            "DELETE FROM hosts WHERE uuid=?",
            [uuid] )
        self.__db.commit()
        self.SEMA.release()
        return {"addr":res[2],"hostname":res[1]}


    # raportowanie stanu sieci


    def host_list(self):
        """
        Return list of all hosts in network
        """
        self.SEMA.acquire()
        self.cur.execute( "SELECT uuid, addr, hostname FROM hosts ORDER BY addr" )
        lst = self.cur.fetchall()
        self.SEMA.release()
        return lst


    def workers_on_host(self, host_uuid):
        addr = self.host_addr_by_uuid(host_uuid)
        if addr==None:
            return []
        self.SEMA.acquire()
        self.cur.execute( "SELECT uuid, service, ip, port, pid FROM workers WHERE ip=?", (addr,) )
        lst = self.cur.fetchall()
        self.SEMA.release()
        return lst


    def host_addr_by_uuid(self, uuid):
        """
        Zwraca adres IP na którym znajduje się host o podanym UUID
        """
        self.SEMA.acquire()
        self.cur.execute( "SELECT addr FROM hosts WHERE uuid=?", (uuid,) )
        host = self.cur.fetchone()
        self.SEMA.release()
        if not host is None:
            return host[0]

    def worker_ip_port_by_uuid(self, uuid, pid=False):
        """
        zwraca ip i port workera o podanym uuid
        """
        self.SEMA.acquire()
        self.cur.execute( "SELECT ip,port,pid FROM workers WHERE uuid=?", (uuid,) )
        wrk = self.cur.fetchone()
        self.SEMA.release()
        if wrk is None:
            return None, None
        else:
            if pid:
                return wrk[0], wrk[1], wrk[2]
            return wrk[0], wrk[1]
