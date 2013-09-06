#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.protocol import serialize, deserialize, messages
from servicebus.conf import settings
from servicebus import exceptions
from servicebus.lib.binder import get_bind_address
from servicebus.lib.comm import RepLoop, send_and_receive
from servicebus.lib import LOG
from datetime import datetime, timedelta
from gevent_zeromq import zmq
import gevent
import random
from pprint import pprint

from servicebus.lib.control_tasks import ControlTasks, RedirectRequiredToAddr

__all__=("SyncWorker",)


def _ip_to_sync_addr(ip):
    return "tcp://%s:%i" % (ip, settings.SYNCD_CONTROL_PORT)

class RedirectRequiredEx(RedirectRequiredToAddr):
    def __init__(self, ip):
        self.remote_addr = _ip_to_sync_addr(ip)



class SyncWorker(object):
    """
    Główna klasa syncd która nasłuchuje na lokalnych socketach i od lokalnych workerów i klientów.
    """

    def __init__(self, server, database, broadcaster):
        self.DAEMON = server
        self.DB = database
        self.BC = broadcaster
        self.context = zmq.Context()
        self.__pingdb = {}
        # local workers <--> local sync communication
        self.queries = RepLoop(self._connect_queries_loop, context=self.context)
        self.queries.register_message(messages.WORKER_LIVE, self.handle_worker_live)
        self.queries.register_message(messages.WORKER_LEAVE, self.handle_worker_leave)
        self.queries.register_message(messages.QUERY, self.handle_name_query)
        self.queries.register_message(messages.CTL_CALL, self.handle_local_control_request)
        # sync <--> sync communication
        self.intersync = RepLoop(self._connect_inter_sync_loop, context=self.context)
        self.intersync.register_message(messages.CTL_CALL, self.handle_global_control_request)
        # service control tasks
        self.ctl = ControlTasks(self.context, allow_redirect=True)
        self.ctl.register_task("host.list", self.CTL_host_list)
        self.ctl.register_task("host.workers", self.CTL_host_workers)
        self.ctl.register_task("worker.stop", self.CTL_worker_stop)


    def get_sync_address(self, ip):
        return "tcp://%s:%i" % (ip, settings.SYNCD_CONTROL_PORT)


    # socket connectors

    def _connect_queries_loop(self, context):
        """
        connect local queries loop
        """
        sock = context.socket(zmq.REP)
        addr = 'ipc://'+settings.SOCK_QUERIES
        sock.bind(addr)
        LOG.debug("Connected query socket %s" % addr)
        return sock, addr

    def _connect_inter_sync_loop(self, context):
        """
        connext global network syncd dialog
        """
        sock = context.socket(zmq.REP)
        addr = _ip_to_sync_addr( get_bind_address(settings.SYNCD_CONTROL_BIND) )
        sock.bind(addr)
        LOG.debug("Connected inter-sync dialog socket %s" % addr)
        return sock, addr


    # closing and quitting

    def stop(self):
        #self.local_input.stop()
        self.queries.stop()

    def close(self):
        #self.local_input.close()
        self.queries.close()

    # all message loops used in syncd
    def get_loops(self):
        return [
            self.queries.loop,
            self.hearbeat_loop,
            self.intersync.loop,
        ]


    # local message handlers
    # -----------------------------------

    def handle_worker_live(self, msg):
        """
        Worker notify about himself
        """
        now = datetime.now()
        uuid = msg['uuid']
        ip = msg['addr']
        port = msg['port']
        svce = msg['service']
        addr = ip+"."+str(port)
        try:
            png = self.__pingdb[addr]
        except KeyError:
            # service is already unknown
            self.worker_start(uuid, svce, ip, port, True)
            return

        if png['s']!=svce:
            # different service under same address!
            LOG.error("Service [%s] expected on address [%s], but [%s] appeared. Removing old, registering new." % (png['s'], addr, svce))
            self.worker_stop(ip, port, True)
            self.worker_start(uuid, svce, ip, port, True)
            return

        # update heartbeats
        png['t'] = now
        self.__pingdb[addr] = png


    def handle_worker_leave(self, msg):
        """
        Worker is going down
        """
        self.worker_stop( msg['ip'], msg['port'], True )

    def handle_name_query(self, msg):
        """
        Odpowiedź na pytanie o adres workera
        """
        name = msg['service']
        res = self.DB.choose_worker_for_service(name)
        if res is None:
            a = None
            p = None
        else:
            a = res[0]
            p = res[1]
        return {
            'message':messages.WORKER_ADDR,
            'service':name,
            'ip':a,
            'port':p
        }

    def handle_local_control_request(self, msg):
        """
        control requests from localhost
        """
        return self.ctl.handle_request(msg)



    # worker state changes
    # ------------------------------

    def worker_start(self, uuid, service, ip, port, local):
        """
        Handle joining to network by worker (local or blobal)
        """
        succ = self.DB.worker_register(uuid, service, ip,port, local)
        addr = ip+":"+str(port)
        if succ:
            if local:
                LOG.info("Local worker [%s] started, address [%s]" % (service, addr) )
            else:
                LOG.info("Remote worker [%s] started, address [%s]" % (service, addr) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            self.__pingdb[addr] =  {
                's' : service,
                't' : datetime.now(),
            }

        # rozesłanie informacji w sieci jeśli nastąpi lokalna zmiana stanu
        if succ and local:
            self.BC.send_worker_live(uuid, service, ip,port)


    def worker_stop(self, ip, port, local):
        """
        Handle worker leaving network (local or global)
        """
        service = self.DB.worker_unregister(ip,port)
        addr = ip+":"+str(port)
        if service:
            if local:
                LOG.info("Local worker [%s] stopped, address [%s]" % (service, addr) )
            else:
                LOG.info("Remote worker [%s] stopped, address [%s]" % (service, addr) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            try:
                del self.__pingdb[addr]
            except KeyError:
                pass
        # rozesłanie informacji w sieci jeśli nastąpi lokalna zmiana stanu
        if local:
            self.BC.send_worker_stop(ip,port)


    def request_workers_broadcast(self):
        """
        Send to all local workers request for registering in network.
        Its used after new host start.
        """
        for uuid, service, ip, port in self.DB.get_local_workers():
            gevent.sleep(0.4)
            self.BC.send_worker_live(uuid, service, ip,port)


    # heartbeat
    def hearbeat_loop(self):
        maxpinglife = timedelta( seconds = settings.HEARTBEAT_TIMEOUT + settings.WORKER_HEARTBEAT )
        unreglist = []
        while True:
            now = datetime.now()
            for addr, nfo in self.__pingdb.iteritems():
                # find outdated timeouts
                to = nfo['t'] + maxpinglife
                if to<now:
                    LOG.warning("Worker [%s] died on address [%s]. Unregistering." % (nfo['s'], addr) )
                    unreglist.append(addr)
            # unregister all dead workers
            while len(unreglist)>0:
                ip,port = unreglist.pop().split(":")
                self.worker_stop(ip,int(port), True)

            gevent.sleep(settings.WORKER_HEARTBEAT)



    # inter sync communication and global management
    # -------------------------------------------------

    def own_ip_or_exception(self, ip):
        """
        Check if given IP is own host ip, if not then raise exception
        to redirect message to proper destination
        """
        if ip is None:
            return
        ownip = self.intersync.ip==ip
        if not ownip:
            raise RedirectRequiredEx(ip)


    def handle_global_control_request(self, message):
        """
        Control requests from remote hosts
        """
        return self.ctl.handle_request(msg)


    # syncd host tasks


    def CTL_host_list(self):
        """
        List of all hosts in service bus
        """
        lst = []
        # all syncd hosts
        for u,a,h in self.DB.host_list():
            ln = {'uuid':u, 'addr':a, 'hostname':h}
            # workers on host
            ln ['services'] = self.CTL_host_workers(u)
            lst.append( ln )
        return lst


    def CTL_host_workers(self, uuid):
        """
        List of all workers on host
        """
        lst = []
        for u,s,i,p in self.DB.workers_on_host(uuid):
            res = {'uuid':u, 'service':s, 'ip':i, 'port':p}
            lst.append(res)
        return lst


    def CTL_worker_stop(self, uuid):
        """
        Send stop signal to worker
        """
        ip,port = self.DB.worker_ip_port_by_uuid(uuid)
        self.own_ip_or_exception(ip)
        if ip is None:
            return False
        #msg = {""}
        addr = "tcp://%s:%i" % (ip,port)
        msg = {
            'message':messages.CTL_CALL,
            'method':['stop']
        }
        res = send_and_receive(self.context, addr, msg)
        print ">>> res", res
        print '   KILL', ip,port
        return "fiku miku suck"
