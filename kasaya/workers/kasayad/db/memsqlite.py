#!/usr/bin/env python
#coding: utf-8
from gevent.coros import Semaphore
from datetime import datetime
import sqlite3 as SQ
from base import DBException, BaseDB


class MemoryDB(BaseDB):

    def __init__(self):
        self.__db = SQ.connect(":memory:")
        self.cur=self.__db.cursor()
        self.cur.execute("CREATE TABLE hosts (uuid TEXT, hostname TEXT, addr TEXT)")
        self.cur.execute("CREATE TABLE workers (uuid TEXT, service TEXT, ip TEXT, port INT, pid INT)")
        self.cur.execute("CREATE TABLE services (host_uuid TEXT, name TEXT)")
        self.SEMA = Semaphore()

    def close(self):
        self.__db.close()


    # hosts
    # -------------------------


    def host_add(self, uuid, addr, hostname):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO hosts ('uuid','addr','hostname') VALUES (?,?,?)",
                (uuid, addr, hostname))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def host_del(self, uuid):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT * FROM hosts WHERE uuid=?",
                (uuid,) )
            self.__db.commit()
        finally:
            self.SEMA.release()


    def host_list(self):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT uuid,addr,hostname FROM hosts")
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for u,a,h in res:
            yield { 'uuid':u, 'addr':a, 'hostname':h }



    def service_add(self, host_uuid, name):
        """
        Dodanie serwisu do hosta
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO services ('host_uuid','name') VALUES (?,?)",
                (host_uuid, name))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def service_del(self, host_uuid, name):
        """
        Usunięcie serwisu z hosta
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM services WHERE host_uuid=? AND name=?",
                (host_uuid, name) )
            self.__db.commit()
        finally:
            self.SEMA.release()


    def service_list(self, host_uuid):
        self.SEMA.acquire()
        try:
            self.cur.execute( "SELECT name FROM services WHERE host_uuid=?",
                (host_uuid,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for s in res:
            yield {'service',s[0]}




    def worker_add(self, worker_uuid, name, ip, port, pid):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO workers ('uuid','ip', 'port', 'service','pid') VALUES (?,?,?,?,?)",
                (worker_uuid, ip, port, name, pid))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def worker_del(self, ip, port):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM workers WHERE ip=? AND port=?",
                [ip,port] )
            self.__db.commit()
        finally:
            self.SEMA.release()


    def worker_list(self, ip):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT uuid,service,ip,port,pid FROM workers WHERE ip=?",
                (ip,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for u,s,i,p,pid in res:
            yield { 'uuid':u, 'name':s, 'ip':i, 'port':p, 'pid':pid }



    #def worker_exist(self, worker_uuid):
    #    self.SEMA.acquire()
    #    try:
    #        self.cur.execute(
    #            "SELECT * FROM workers WHERE uuid=?",
    #            [worker_uuid] )
    #        res = self.cur.fetchone()
    #    finally:
    #        self.SEMA.release()
    #    return res is not None
