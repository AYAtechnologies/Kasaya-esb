#!/usr/bin/env python
#coding: utf-8
import random
__all__ = ("DBException", "BaseDB")



class DBException(Exception):
    pass


class BaseDB(object):

    # Jeśli True, to broadcast nie będzie używany.
    # Baza danych musi na wszystkich hostach w sieci
    # reprezentować identyczny stan.
    replaces_broadcast = False

    def set_own_ip(self, ip):
        self.own_ip=ip

    def connect(self):
        pass

    def close(self):
        pass


    # hosts
    # -------------------------


    def host_add(self, uuid, addr, hostname):
        raise NotImplemented()


    def host_del(self, uuid):
        raise NotImplemented()


    def host_list(self):
        raise NotImplemented()


    def host_exist(self, host_uuid=None, addr=None):
        """
        Check if host exists in database.
        """
        if host_uuid == addr == None:
            return None
        for h in self.host_list():
            if host_uuid is not None:
                if h['uuid']==host_uuid:
                    return h
            if addr is not None:
                if h['addr']==addr:
                    return h


    # services
    # -------------------------


    def service_add(self, host_uuid, name):
        raise NotImplemented()


    def service_del(self, host_uuid, name):
        raise NotImplemented()


    def service_list(self, host_uuid):
        raise NotImplemented()


    # workers
    # -------------------------


    def worker_add(self, worker_uuid, name, ip, port, pid):
        raise NotImplemented()


    def worker_del(self, ip, port):
        raise NotImplemented()


    def worker_list(self, ip):
        raise NotImplemented()


    def worker_list_local(self):
        for w in self.worker_list(self.own_ip):
            yield w

    def worker_exist(self, worker_uuid):
        raise NotImplemented()
