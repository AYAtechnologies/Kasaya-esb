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
        return SyncWorker(server=self)


    def setup_broadcaster(self):
        backend = settings.SYNC_BACKEND
        if backend=="udp-broadcast":
            from broadcast.udp import UDPBroadcast
            return UDPBroadcast(server=self)
        raise Exception("Unknown broadcast backend: %s" % backend)


    def run(self):
        try:
            gevent.joinall([
                gevent.spawn(self.WORKER.run_local_loop),
                gevent.spawn(self.WORKER.run_query_loop),
                gevent.spawn(self.BC.run_listener),
            ])
        finally:
            self.DB.close()
            self.WORKER.close()
            self.BC.close()



