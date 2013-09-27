#!/usr/bin/env python
#coding: utf-8
from gevent.coros import Semaphore
from datetime import datetime
import sqlite3 as SQ
#from base import DBException


class MemoryDB(object):

    def __init__(self):
        self.__db = SQ.connect(":memory:")
        self.cur=self.__db.cursor()
        self.cur.execute("CREATE TABLE hosts (uuid TEXT, hostname TEXT, addr TEXT)")
        self.cur.execute("CREATE TABLE workers (uuid TEXT, service TEXT, ip TEXT, port INT, pid INT)")
        self.cur.execute("CREATE TABLE services (host_uuid TEXT, name TEXT)")
        self.SEMA = Semaphore()

    def set_own_ip(self, ip):
        self.__ip==ip

    def connect(self):
        pass

    def close(self):
        self.__db.close()



    # -----------



    def host_add(self, uuid, addr, hostname):
        self.SEMA.acquire()
        self.cur.execute(
            "INSERT INTO hosts ('uuid','addr','hostname') VALUES (?,?,?)",
            (host_uuid, addr, hostname))
        self.__db.commit()
        self.SEMA.release()


    def host_del(self, uuid):
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT * FROM hosts WHERE uuid=?",
            (uuid,) )
        self.__db.commit()
        self.SEMA.release()


    def host_list(self):
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT uuid,addr,hostname FROM hosts",
            [host_uuid] )
        res = self.cur.fetchall()
        self.SEMA.release()
        for u,a,h in res:
            yield { 'uuid':u, 'addr':a, 'hostname':h }




    def service_add(self, host_uuid, name):
        """
        Dodanie serwisu do hosta
        """
        self.SEMA.acquire()
        self.cur.execute(
            "INSERT INTO services ('host_uuid','name') VALUES (?,?)",
            (host_uuid, name))
        self.__db.commit()
        self.SEMA.release()


    def service_del(self, host_uuid, name):
        """
        Usunięcie serwisu z hosta
        """
        self.SEMA.acquire()
        self.cur.execute(
            "DELETE FROM services WHERE host_uuid=? AND name=?",
            (host_uuid, name) )
        self.__db.commit()
        self.SEMA.release()


    def service_list(self, host_uuid):
        self.SEMA.acquire()
        self.cur.execute( "SELECT name FROM services WHERE host_uuid=?",
            (host_uuid,) )
        res = self.cur.fetchall()
        self.SEMA.release()
        for s in res:
            yield {'service',s[0]}




    def worker_add(self, worker_uuid, name, ip, port, pid):
        self.SEMA.acquire()
        self.cur.execute(
            "INSERT INTO workers ('uuid','ip', 'port', 'service','pid') VALUES (?,?,?,?,?)",
            (worker_uuid, ip, port, name, pid))
        self.__db.commit()
        self.SEMA.release()


    def worker_del(self, ip, port):
        self.SEMA.acquire()
        self.cur.execute(
            "DELETE FROM workers WHERE ip=? AND port=?",
            [ip,port] )
        self.__db.commit()
        self.SEMA.release()


    def worker_list(self, ip, port):
        self.SEMA.acquire()
        self.cur.execute(
            "SELECT uuid,service,ip,port,pid FROM workers WHERE ip=? AND port=?",
            (ip, port) )
        res = self.cur.fetchall()
        self.SEMA.release()
        for u,s,i,p,pid in res:
            yield { 'uuid':u, 'name':s, 'ip':i, 'port':p, 'pid':pid }

