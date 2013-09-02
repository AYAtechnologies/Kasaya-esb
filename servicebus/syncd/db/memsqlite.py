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
        self.cur.execute("CREATE TABLE workers (uuid TEXT, service TEXT, ip TEXT, port INT, local INT)")
        self.SEMA = Semaphore()

    def close(self):
        self.__db.close()



    def worker_register(self, worker_uuid, service_name, ip,port, localservice):
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
        # dodanie nowego workera do bazy
        self.SEMA.acquire()
        self.cur.execute(
            "INSERT INTO workers ('uuid','ip','port', 'service','local') VALUES (?,?,?,?,?)",
            (worker_uuid, ip,port, service_name, localservice))
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
            "SELECT ip,port FROM workers WHERE local=?",
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
        self.cur.execute( "SELECT uuid, hostname, addr FROM hosts" )
        lst = self.cur.fetchall()
        self.SEMA.release()
        return lst

    def workers_on_host(self, host_uuid):
        self.SEMA.acquire()
        self.cur.execute( "SELECT addr FROM hosts WHERE uuid=?", (host_uuid,) )
        host = self.cur.fetchone()
        self.SEMA.release()

        if host==None:
            return []

        self.SEMA.acquire()
        self.cur.execute( "SELECT uuid, service, addr FROM workers WHERE addr=?", (host[0],) )
        lst = self.cur.fetchall()
        self.SEMA.release()
        return lst


        #haddr = self.hosts[host_uuid]
        #haddr = haddr['addr'].rsplit(":",1)[0]
        #for svce, addrlist in self.services.iteritems():
        #    for addr in addrlist:
        #        if addr.rsplit(":",1)[0]==haddr:
        #            yield svce, addr


    def uuid2addr(self, uuid):
        """
        Zwraca adres IP na którym znajduje się host/worker o podanym UUID
        """
        pass