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


    def close(self):
        pass


    # worker management
    # -----------------


    def worker_register(self, worker_uuid, name, addr, is_local_service):
        """
        Zarejestrowanie workera w bazie.
          uuid - identyfikator uuid workera
          name - nazwa serwisu
          addr - adres ip workera
          port - numer portu workera
          localservice - worker jest na lokalnym hoście
        """
        raise NotImplemented


    def worker_unregister(self, addr=None):
        """
        Wyrejestrowanie workera z bazy.
          Zwraca True jeśli wyrejestrowano workera,
          False jeśli workera nie było w bazie
        """
        raise NotImplemented


    def get_local_workers(self):
        """
        Return list of live workers on local host
        """
        raise NotImplemented


    def get_workers_for_service(self, service_name):
        """
        Zwraca listę wszystkich workerów które realizują podany serwis
        """
        raise NotImplemented


    def choose_worker_for_service(self, service_name):
        """
        Losuje / wybiera workera który realizuje usługę o podanej nazwie.
        Jeśli podany serwis nie istnieje, wynikiem jest None
        """
        workers = self.get_workers_for_service(service_name)
        if len(workers)<1:
            return None
        else:
            return random.choice( workers )


    # global network state - registry of all syncd servers in network
    # ---------------------------------------------------------------

    def host_register(self, host_uuid, addr, hostname ):
        raise NotImplemented

    def host_unregister(self, host_uuid):
        raise NotImplemented


    # network state reporting
    # -----------------------


    def uuid2addr(self, uuid):
        """
        Zwraca adres IP na którym znajduje się host/worker o podanym UUID
        """
        raise NotImplemented


    def host_list(self):
        """
        Return list of all hosts in network
        """
        raise NotImplemented


    def workers_on_host(self, host_uuid):
        raise NotImplemented

