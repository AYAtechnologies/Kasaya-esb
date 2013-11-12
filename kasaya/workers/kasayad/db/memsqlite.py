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
        self.cur.execute("CREATE TABLE hosts (id TEXT, hostname TEXT, ip TEXT)")
        self.cur.execute("CREATE TABLE workers (id TEXT, host_id TEXT, service TEXT, addr TEXT, pid INT)")
        self.cur.execute("CREATE TABLE services (host_id TEXT, name TEXT)")
        self.SEMA = Semaphore()

    def close(self):
        self.__db.close()


    # hosts
    # -------------------------


    def host_add(self, ID, ip, hostname):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO hosts ('id','ip','hostname') VALUES (?,?,?)",
                (ID, ip, hostname))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def host_get(self, ID):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT ID,ip,hostname FROM hosts WHERE id=?",
                (ID,) )
            res = self.cur.fetchone()
        finally:
            self.SEMA.release()
        if res is None:
            return None
        return { 'id':res[0], 'ip':res[1], 'hostname':res[2] }


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
                "SELECT id,ip,hostname FROM hosts")
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for u,a,h in res:
            yield { 'id':u, 'ip':a, 'hostname':h }



    def service_add(self, host_ID, name):
        """
        Dodanie serwisu do hosta
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO services ('host_id','name') VALUES (?,?)",
                (host_ID, name))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def service_del(self, host_ID, name):
        """
        Usunięcie serwisu z hosta
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "DELETE FROM services WHERE host_id=? AND name=?",
                (host_ID, name) )
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




    def worker_add(self, host_id, worker_id, name, address, pid):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "INSERT INTO workers ('id', 'host_id', 'addr', 'service','pid') VALUES (?,?,?,?,?)",
                (worker_id, host_id, address, name, pid))
            self.__db.commit()
        finally:
            self.SEMA.release()


    def worker_get(self, worker_id):
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT ID,host_id,service,addr,pid FROM workers WHERE id=?",
                [worker_id,] )
            res = self.cur.fetchone()
        finally:
            self.SEMA.release()

        if res is None:
            return
        return { 'id':res[0], 'host_id':res[1], 'service':res[2], 'addr':res[3], 'pid':res[4] }


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
                "SELECT id,service,addr,pid, host_id FROM workers WHERE host_id=?",
                (host_id,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        if res is None:
            return
        for i,s,a,pid, hh in res:
            yield { 'id':i, 'service':s, 'addr':a, 'pid':pid }


    def workers_for_service(self, service):
        """
        List of workers for service
        """
        self.SEMA.acquire()
        try:
            self.cur.execute(
                "SELECT id,addr FROM workers WHERE service=?",
                (service,) )
            res = self.cur.fetchall()
        finally:
            self.SEMA.release()
        for i,a in res:
            yield { 'id':i, 'addr':a }


    #def worker_list_local_services(self, host_id, service):
    #    self.SEMA.acquire()
    #    try:
    #        self.cur.execute(
    #            "SELECT ip,port FROM workers WHERE ip=? AND service=?",
    #            (ip, service) )
    #        res = self.cur.fetchall()
    #    finally:
    #        self.SEMA.release()
    #    lst = []
    #    for s in res:
    #        lst.append( {'ip':s[0], 'port':s[1]} )
    #    return lst



    #def worker_exist(self, worker_ID):
    #    self.SEMA.acquire()
    #    try:
    #        self.cur.execute(
    #            "SELECT * FROM workers WHERE ID=?",
    #            [worker_ID] )
    #        res = self.cur.fetchone()
    #    finally:
    #        self.SEMA.release()
    #    return res is not None
