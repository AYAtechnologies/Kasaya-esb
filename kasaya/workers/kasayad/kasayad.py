#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
# imports
from kasaya.conf import settings
from kasaya.core.lib import LOG, system
from kasaya.core.events import add_event_handler, emit
from kasaya.core.worker.worker_base import WorkerBase
from .syncworker import SyncWorker
from .db.netstatedb import NetworkStateDB
#from .broadcast import UDPLoop
from kasaya.core.protocol.comm import UDPMessageLoop, send_without_response
from kasaya.core.protocol import messages
from . import netsync
import gevent



class KasayaNetworkSyncIO(netsync.KasayaNetworkSync):
    """
    KasayaNetworkSync instance with working send/broadcast methods
    """
    def __init__(self, parent, DB, ID, hostname):
        self.parent = parent
        super(KasayaNetworkSyncIO, self).__init__(DB, ID, hostname)
        self.known_hosts_dump_file = settings.KASAYAD_HOST_LIST_DUMP_FILE

    def send_broadcast(self, data):
        self.parent.send_kasaya_broadcast( messages.net_sync_message(data) )

    def send_message(self, addr, data):
        #LOG.debug ("SEND " + str(addr) +"  "+ repr(data) )
        self.parent.send_kasaya_message(addr, messages.net_sync_message(data) )



class KasayaDaemon(WorkerBase):

    def __init__(self):
        super(KasayaDaemon, self).__init__("kasayad", is_host=True)
        self.hostname = system.get_hostname()

        # event handlers
        add_event_handler("host-join", self.on_remote_kasayad_start)
        add_event_handler("host-leave", self.on_remote_kasayad_stop)
        #add_event_handler("local-worker-on", self.on_local_worker_start)
        #add_event_handler("local-worker-off", self.on_local_worker_start)

        self.DB = NetworkStateDB()  # database
        self.BC = UDPMessageLoop(settings.BROADCAST_PORT, self.ID) # broadcaster
        self.BC.register_message(messages.NET_SYNC, self.proc_sync_message)

        self.SYNC = KasayaNetworkSyncIO(self, self.DB, self.ID, system.get_hostname() )
        self.WORKER = SyncWorker(server=self, database=self.DB)
        #self.WMAN = WorkerManagement()

    def _local_worker_addr(self, port):
        return "tcp://127.0.0.1:%i" % port

    def start(self):
        """
        Run internal all services, register self in db and send network broadcast.
        """
        LOG.info("Starting local kasaya daemon with ID: [%s]" % self.ID)
        # register self in database
        self.DB.host_register(self.ID, "tcp://127.0.0.1:%i" % settings.KASAYAD_CONTROL_PORT, self.hostname )
        # register own services
        self.DB.service_update_list(self.ID, self.WORKER.local_services_list() )
        # connect to network
        self.SYNC.start()


    def close(self):
        """
        Notifies network about shutting down, closes database
        and all used sockets.
        """
        LOG.info("Stopping local kasaya daemon [id:%s]"%self.ID)
        self.DB.host_unregister(self.ID)
        self.SYNC.close()
        #self.on_local_kasayad_stop(self.ID, local=True)
        self.WORKER.close()
        self.DB.close()
        self.BC.close()


    # network state IO

    def send_kasaya_broadcast(self, message):
        self.BC.broadcast_message(message)

    def send_kasaya_message(self, addr, message):
        send_without_response(addr, message)

    # incoming messages
    def proc_sync_message(self, addr, msg):
        LOG.debug ("received " + str(addr) +"  "+ repr(msg) )
        senderaddr = "tcp://%s:%i" % (addr[0], settings.KASAYAD_CONTROL_PORT)
        self.SYNC.receive_message(senderaddr, msg['data'])


    # event driven worker management
    def __send_message_to_worker(self, worker_id, message):
        """
        Send control message to local worker
        """
        worker = self.DB.worker_get(worker_id)


    def local_worker_add(self, worker_id, address, service, pid):
        """
        Local worker started in offline state.
        on event: worker-local-add
        emit: worker-local-start
        """
        self.DB.worker_register(self.ID, worker_id, service, address, pid, online=False)
        LOG.info("Local worker [%s] started, address [%s] [id:%s]" % (service, address, worker_id) )
        emit("worker-local-start", worker_id)

    def worker_start(self, worker_id):
        """
        Set worker online
        """
        worker = self.DB.worker_get(worker_id)
        if worker['id']!=self.ID:
            LOD.error("Local worker is registered under wrong host_id")
            return
        # configure new worker
        params = {}
        res = send_and_receive_response(worker['addr'], msg)
        self.DB.worker_set_state( worker_id, True )
        self.SYNC.local_worker_add(worker_id, service, address)

    def worker_stop(self, worker_id):
        """
        Set worker offline
        """

    def worker_del(self):
        """
        Remove worker
        """



    def service_add(self):
        pass

    def service_del(self):
        pass


    # global network changes

    def on_remote_kasayad_start(self, host_id, addr, hostname):
        """
        Remote kasaya host started
        """
        # register self in database
        LOG.info("Remote kasaya daemon started, address: %s [id:%s]" % (addr, host_id) )

    def on_remote_kasayad_stop(self, host_id):
        """
        received information about kasaya host leaving network
        """
        LOG.info("Remote kasaya daemon stopped, [id:%s]" % host_id)


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
        # start after loops run
        g=gevent.Greenlet( self.start )
        g.start()
        # run all loops
        try:
            loops = []
            loops.append(self.BC.loop)
            loops.append(self.WORKER.loop)
            loops = [ gevent.spawn(loop) for loop in loops ]
            gevent.joinall(loops)
        finally:
            self.close()


