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

    #def set_own_ip(self, ip):
    #    self.own_ip=ip

    def connect(self):
        pass

    def close(self):
        pass


    # hosts
    # -------------------------


    def host_add(self, ID, ip, hostname):
        raise NotImplemented()

    def host_get(self, ID):
        raise NotImplemented()

    def host_del(self, ID):
        raise NotImplemented()

    def host_list(self):
        raise NotImplemented()

    def host_exist(self, host_id=None, ip=None):
        """
        Check if host exists in database.
        """
        if host_id == ip == None:
            return None
        for h in self.host_list():
            if host_id is not None:
                if h['id']==host_id:
                    return h
            if ip is not None:
                if h['ip']==ip:
                    return h


    # services
    # -------------------------


    def service_add(self, host_id, name):
        raise NotImplemented()


    def service_del(self, host_id, name):
        raise NotImplemented()


    def service_list(self, host_id):
        raise NotImplemented()



    # workers
    # -------------------------


    def worker_add(self, worker_id, name, ip, port, pid):
        raise NotImplemented()


    def worker_get(self, worker_id):
        raise NotImplemented()


    def worker_del(self, ip, port):
        raise NotImplemented()


    def worker_list(self, ip):
        raise NotImplemented()

    def worker_exist(self, worker_id):
        raise NotImplemented()
