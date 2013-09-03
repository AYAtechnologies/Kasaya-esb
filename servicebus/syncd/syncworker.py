#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.protocol import serialize, deserialize, messages
from servicebus.conf import settings
from servicebus import exceptions
from servicebus.lib.binder import get_bind_address
from servicebus.lib.loops import RepLoop
from servicebus.lib import LOG
from datetime import datetime, timedelta
from gevent_zeromq import zmq
import gevent
import random



class TooLong(Exception): pass





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
        self.__ctltasks = {}
        self.register_ctltask("host.list", self.CTL_host_list, False)
        self.register_ctltask("host.workers", self.CTL_host_workers, False)


    def register_ctltask(self, method, func, redirect):
        """
        Register control method.

        Jeśli redirect jest True, oznacza to że żądanie może zostać zrealizowane tylko przez
        serwer syncd którego dotyczy polecenie, jeśli False, to może zostać zrealizwowane
        na każdym serwerze syncd w sieci.
        """
        self.__ctltasks[method] = (func, redirect)


    def get_sync_address(self, addr):
        return "tcp://%s:%i" % (addr, settings.SYNCD_CONTROL_PORT)


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
        addr = self.get_sync_address( get_bind_address(settings.SYNCD_CONTROL_BIND) )
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
        return self.handle_control_request(msg, islocal=True)



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
        if succ and local:
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


    def send_to_another_sync(self, addr, message):
        """
        Send request to another syncd server
        """
        sock = self.context.socket(zmq.REQ)
        sock.connect( self.get_sync_address(addr) )
        sock.send( serialize(msg) )
        res = sock.sync_sender.recv()
        res = deserialize(res)
        sock.close()
        return res


    def handle_global_control_request(self, message):
        """
        Control requests from remote hosts
        """
        return handle_control_request(message, islocal=False)


    def handle_control_request(self, message, islocal):
        """
        All control requests are handled here.
           message - message body
           islocal - if true then request is from localhost
        """
        method = ".".join(message['method'])
        LOG.debug("Management call [%s]" % method)
        # get handler for method
        func, redirect = self.__ctltasks[method]

        if redirect:
            # this request can be called only on localhost
            # if target uuid is not for this syncd instance
            # then we should redirect it to remote host
            raise NotImplemented("redirecting requests is not ready yet")

        # call internal function
        result = func(*message['args'], **message['kwargs'])
        return {"message":messages.RESULT, "result":result }


    # host tasks


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