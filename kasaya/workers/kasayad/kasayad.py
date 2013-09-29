#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.lib import LOG, system
from .syncworker import SyncWorker
from .db.netstatedb import NetworkStateDB
import gevent, uuid


class KasayaDaemon(object):

    def __init__(self):
        self.hostname = system.get_hostname()
        self.uuid = str( uuid.uuid4() )
        LOG.info("Starting local sync daemon with uuid: [%s]" % self.uuid)
        # uruchomienie bazy danych
        #self.DB = self.setup_db()
        self.DB = NetworkStateDB()
        # broadcaster is not used with distributed database backend
        if not self.DB.replaces_broadcast:
            self.BC = self.setup_broadcaster()
        else:
            from broadcast.fake import FakeBroadcast
            self.BC = FakeBroadcast()
        # uruchomienie workera
        self.WORKER = self.setup_worker()
        self.DB.set_own_ip(self.WORKER.own_ip)
        self.BC.set_own_ip(self.WORKER.own_ip)



    def close(self):
        """
        Notifies network about shutting down, closes database
        and all used sockets.
        """
        LOG.info("Stopping local sync daemon")
        self.notify_syncd_stop(self.uuid, local=True)
        self.WORKER.close()
        self.DB.close()
        self.BC.close()


    # setting up daemon


    def setup_worker(self):
        """
        This worker loop is for communication between syncd and local workers.
        """
        return SyncWorker(server=self, database=self.DB, broadcaster=self.BC)


    def setup_broadcaster(self):
        """
        Broadcaster is used to synchronise information between all syncd servers in network.
        """
        backend = settings.SYNC_BACKEND
        if backend=="udp-broadcast":
            from .broadcast.udp import UDPBroadcast
            return UDPBroadcast(server=self)
        raise Exception("Unknown broadcast backend: %s" % backend)


    # global network changes

    def notify_syncd_start(self, uuid, hostname, addr, services, local=False):
        """
        Send information about startup of host to all other hosts in network
        """
        succ = self.DB.host_register(uuid, hostname, addr, services)
        self.WORKER.request_workers_broadcast()
        if local:
            # it is ourself starting, send broadcast about this
            self.BC.send_host_start(uuid, hostname, addr, services)
        else:
            # it's remote host starting, information is from broadcast
            if succ:
                a = str(addr)
                LOG.info("Remote sync host [%s] started, address [%s], uuid [%s]" % (hostname, a, uuid) )

        # if registered new syncd AND it's not local host, then
        # it must be new host in network, which don't know other hosts.
        # We send again registering information about self syncd instance.
        if succ and (not local):
            gevent.sleep(0.5)
            self.notify_syncd_self_start()


    def notify_syncd_self_start(self):
        """
        send information about self start to all hosts
        """
        addr = self.WORKER.intersync.address
        addr = addr.split(":")[1].lstrip("/")
        self.notify_syncd_start(
            self.uuid,
            self.hostname,
            addr,
            self.WORKER.local_services_list(),
            local=True,
        )


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
        self.notify_syncd_self_start()
        try:
            loops = self.WORKER.get_loops()
            loops.append(self.BC.loop)
            loops = [ gevent.spawn(loop) for loop in loops ]
            gevent.joinall(loops)
        finally:
            self.close()
