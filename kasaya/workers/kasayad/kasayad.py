#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.lib import LOG, system
from kasaya.core.events import add_event_handler
from kasaya.core.worker.worker_base import WorkerBase
from .syncworker import SyncWorker
from .db.netstatedb import NetworkStateDB
import gevent


class KasayaDaemon(WorkerBase):

    def __init__(self):
        super(KasayaDaemon, self).__init__(is_host=True)

        # event handlers
        add_event_handler("host-join", self.on_remote_kasayad_start)
        add_event_handler("host-leave", self.on_remote_kasayad_stop)

        self.hostname = system.get_hostname()
        LOG.info("Starting local kasaya daemon with ID: [%s]" % self.ID)
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
        #self.DB.set_own_ip(self.WORKER.own_ip)
        self.BC.set_own_ip(self.WORKER.own_ip)



    def close(self):
        """
        Notifies network about shutting down, closes database
        and all used sockets.
        """
        LOG.info("Stopping local kasaya daemon")
        self.on_local_kasayad_stop(self.ID, local=True)
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
            return UDPBroadcast(self.ID)
        raise Exception("Unknown broadcast backend: %s" % backend)


    # global network changes

    def notify_kasayad_start(self, ID, hostname, ip, services, local=False):
        """
        Send information about startup of host to all other hosts in network.
        """
        isnew = self.DB.host_register(ID, hostname, ip, services)
        if local:
            # it is ourself starting, send broadcast to other kasaya daemons
            self.BC.send_host_start(ID, hostname, ip, services)

        if isnew:
            # new kasayad
            # send request to local workers to send immadiately ping broadcast
            # to inform new kasaya daemon about self
            #self.WORKER.request_workers_broadcast()
            # it's remote host starting, information is from broadcast
            LOG.info("Remote kasaya daemon [%s] started, address [%s], ID [%s]" % (hostname, ip, ID) )
            # if registered new kasayad AND it's not local host, then
            # it must be new host in network, which don't know other hosts.
            # We send again registering information about self syncd instance.
            gevent.sleep(0.5)
            self.notify_kasayad_self_start()


    def on_remote_kasayad_start(self, host_id, addr):
        """
        Remote kasaya host started
        """
        # register self in database
        self.DB.host_register(host_id, addr)
        LOG.info("Remote kasaya daemon started, address: %s [id:%s]" % (addr, host_id) )

    def on_local_kasayad_start(self):
        """
        send information about self start to all hosts
        """
        # register self in database
        self.DB.host_register(self.ID, self.WORKER.own_addr)
        # register own services
        self.DB.service_update_list(self.ID, self.WORKER.local_services_list() )
        # broadcast own existence
        self.BC.broadcast_host_start(self.WORKER.own_addr)


    def on_remote_kasayad_stop(self, host_id):
        """
        received information about kasaya host leaving network
        """
        self.DB.host_unregister(self.ID)
        LOG.info("Remote kasaya daemon stopped, [id:%s]" % host_id)


    def on_local_kasayad_stop(self, ID, local=False):
        """
        Send information about shutdown to all hosts in network
        """
        self.DB.host_unregister(self.ID)
        self.BC.broadcast_host_stop()



    def notify_kasayad_refresh(self, ID, services=None, local=False):
        """
        Received information on host changes
        """
        if services is not None:
            slst = ", ".join(services)
            if local:
                # local changes require broadcast new service status
                self.BC.send_host_refresh(self.ID, services=services)
                LOG.info("Local service list changed [%s]" % slst)
            else:
                # remote host services requires daabase update
                # local updates are entered to database
                # before notify_kasayad_refresh is called
                self.DB.service_update_list(self.ID, services)
                LOG.info("Remote host service list changed [%s]" % slst)



    # main loop
    def run(self):
        self.on_local_kasayad_start()
        try:
            loops = self.WORKER.get_loops()
            loops.append(self.BC.loop)
            loops = [ gevent.spawn(loop) for loop in loops ]
            gevent.joinall(loops)
        finally:
            self.close()


