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
        self.cur.execute("CREATE TABLE hosts (id TEXT, addr TEXT, hostname TEXT)")
        self.cur.execute("CREATE TABLE workers (id TEXT, host_id TEXT, service TEXT, addr TEXT, pid INT, online BOOLEAN)")
        self.cur.execute("CREATE TABLE services (host_id TEXT, name TEXT)")
        self.SEMA = Semaphore()

    def close(self):
        self.__db.close()


    # hosts
    # -------------------------


    def host_add(self, ID, address, hostname):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO hosts ('id','addr','hostname') VALUES (?,?,?)",
                (ID, address, hostname))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def host_get(self, ID):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT ID,addr,hostname FROM hosts WHERE id=?",
                (ID,) )
            res = self.cur.fetchone()
        finally:
            self.SEMA.release()
        if res is None:
            return None
        return { 'id':res[0], 'addr':res[1], 'hostname':res[2] }


    def host_del(self, ID):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM hosts WHERE id=?",
                (ID,) )
            self.__db.commit()
        finally:
            self.SEMA.release()


    def host_list(self):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT id,addr,hostname FROM hosts")
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for u,a,h in res:
            yield { 'id':u, 'addr':a, 'hostname':h }



    def service_add(self, host_id, name):
        """
        Dodanie serwisu do hosta
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO services ('host_id','name') VALUES (?,?)",
                (host_id, name))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def service_del(self, host_id, name):
        """
        Usunięcie serwisu z hosta
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM services WHERE host_id=? AND name=?",
                (host_id, name) )
            self.__db.commit()
        finally:
            self.SEMA.release()


    def service_list(self, host_id):
        """
        List of services on host
        """
        self.SEMA.acquire()
        try:
            self.cur.execute( "SELECT name FROM services WHERE host_id=?",
                (host_id,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for s in res:
            yield {'service':s[0]}




    def worker_add(self, host_id, worker_id, name, address, pid, online):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO workers ('id', 'host_id', 'addr', 'service','pid', 'online') VALUES (?,?,?,?,?,?)",
                (worker_id, host_id, address, name, pid, online) )
            self.__db.commit()
        finally:
            self.SEMA.release()

    def worker_set_state(self, worker_id, is_online):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "UPDATE workers SET online=? WHERE id=?;",
                (is_online, worker_id) )
            self.__db.commit()
        finally:
            self.SEMA.release()


    def worker_get(self, worker_id):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT id,host_id,service,addr,pid, online FROM workers WHERE id=?",
                [worker_id,] )
            res = self.cur.fetchone()
        finally:
            self.SEMA.release()

        if res is None:
            return
        return { 'id':res[0], 'host_id':res[1], 'service':res[2], 'addr':res[3], 'pid':res[4], 'online':res[5]>0 }


    def worker_del_by_addr(self, addr):
        """
        Delete worker from database identified by address
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM workers WHERE addr=?",
                (addr,) )
            self.__db.commit()
        finally:
            self.SEMA.release()

    def worker_del_by_id(self, id):
        """
        Delete worker from database identified by worker id
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM workers WHERE id=?",
                (id,) )
            self.__db.commit()
        finally:
            self.SEMA.release()



    def worker_list(self, host_id):
        '''
        list all workers on given host
        '''
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT id,service,addr,pid, online FROM workers WHERE host_id=?",
                (host_id,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        if res is None:
            return
        for i,s,a,pid,o in res:
            yield { 'id':i, 'service':s, 'addr':a, 'pid':pid, 'online':o>0 }


    def workers_for_service(self, service, only_online=True):
        """
        List of workers for service
        """
        self.SEMA.acquire()
        try:
            if only_online:
                self.cur.execute(
                    "SELECT id,addr FROM workers WHERE service=? AND online=?",
                    (service,1) )
            else:
                self.cur.execute(
                    "SELECT id,addr FROM workers WHERE service=?",
                    (service,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for i,a in res:
            yield { 'id':i, 'addr':a }

