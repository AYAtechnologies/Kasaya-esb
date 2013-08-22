#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
import gevent
from syncworker import SyncWorker



class SyncDaemon(object):

    def __init__(self):
        # uruchomienie bazy danych
        self.DB = self.setup_db()
        # broadcaster is not used with distributed database backend
        if not self.DB.replaces_broadcast:
            self.BC = self.setup_broadcaster()
        else:
            from broadcast.fake import FakeBroadcast
            self.BC = FakeBroadcast()
        # uruchomienie workera
        self.WORKER = self.setup_worker()


    def close(self):
        """
        Notifies network about shutting down, closes database
        and all used sockets.
        """
        self.notify_host_stop(local=True)
        self.DB.close()
        self.WORKER.close()
        self.BC.close()


    # setting up daemon


    def setup_db(self):
        """
        konfiguracja bazy danych
        """
        backend = settings.DB_BACKEND
        if backend=="dict":
            from db.dict import DictDB
            return DictDB(server=self)
        raise Exception("Unknown database backend: %s" % backend)


    def setup_worker(self):
        """
        This worker loop is for communication between syncd and local workers.
        """
        return SyncWorker(server=self)


    def setup_broadcaster(self):
        """
        Broadcaster is used to synchronise information between all syncd servers in network.
        """
        backend = settings.SYNC_BACKEND
        if backend=="udp-broadcast":
            from broadcast.udp import UDPBroadcast
            return UDPBroadcast(server=self)
        raise Exception("Unknown broadcast backend: %s" % backend)


    # global network changes


    def notify_host_start(self, local=False):
        """
        Send information about startup to all hosts in network
        """
        self.WORKER.request_workers_register()
        if local:
            self.BC.send_host_start()

    def notify_host_stop(self, local=False):
        """
        Send information about shutdown to all hosts in network
        """
        pass




    def run(self):
        self.notify_host_start(local=True)
        try:
            gevent.joinall([
                gevent.spawn(self.WORKER.run_local_loop),
                gevent.spawn(self.WORKER.run_query_loop),
                gevent.spawn(self.WORKER.run_hearbeat_loop),
                gevent.spawn(self.BC.run_listener),
            ])
        finally:
            self.close()
            print "Gently finished..."
