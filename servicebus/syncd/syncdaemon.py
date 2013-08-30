#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.conf import settings
from servicebus.lib import LOG, system
import gevent, uuid
from syncworker import SyncWorker


class SyncDaemon(object):

    def __init__(self):
        self.hostname = system.get_hostname()
        self.uuid = str( uuid.uuid4() )
        LOG.info("Starting local sync daemon with uuid: [%s]" % self.uuid)
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
        LOG.info("Stopping local sync daemon")
        self.notify_syncd_stop(self.uuid, local=True)
        self.DB.close()
        self.WORKER.close()
        self.BC.close()


    # setting up daemon


    def setup_db(self):
        """
        konfiguracja bazy danych
        """
        backend = settings.SYNC_DB_BACKEND
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


    def notify_syncd_start(self, uuid, hostname, addr, local=False):
        """
        Send information about startup to all hosts in network
        """
        succ = self.DB.host_register(uuid, hostname, addr)
        self.WORKER.request_workers_register()
        if local:
            self.BC.send_host_start(uuid, hostname, addr)
        else:
            if succ:
                a = str(addr)
                LOG.info("Remote sync host [%s] started, address [%s], uuid [%s]" % (hostname, a, uuid) )


    def notify_syncd_stop(self, uuid, local=False):
        """
        Send information about shutdown to all hosts in network
        """
        res = self.DB.host_unregister(uuid)

        if local:
            self.BC.send_host_stop(uuid)
        else:
            if not res is None:
                a,h = str(res['addr']), res['hostname']
                LOG.info("Remote sync host [%s] stopped, addres [%s], uuid [%s]" % (h, a, uuid))

    # main loop
    def run(self):
        self.notify_syncd_start(
            self.uuid,
            self.hostname,
            self.WORKER.intersync.address,
            local=True
        )
        try:
            loops = self.WORKER.get_loops()
            loops.append(self.BC.loop)
            loops = [ gevent.spawn(loop) for loop in loops ]
            gevent.joinall(loops)
        finally:
            self.close()
