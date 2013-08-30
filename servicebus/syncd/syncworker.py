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
        self.queries.register_message(messages.WORKER_JOIN,  self.handle_worker_join)
        self.queries.register_message(messages.WORKER_LEAVE, self.handle_worker_leave)
        self.queries.register_message(messages.PING, self.handle_ping)
        self.queries.register_message(messages.QUERY,  self.handle_name_query)
        self.queries.register_message(messages.CTL_CALL, self.handle_control_request)
        # sync <--> sync communication
        self.intersync = RepLoop(self._connect_inter_sync_loop, context=self.context)
        self.intersync.register_message(messages.CTL_CALL, self.handle_inter_sync)
        # service control tasks
        self.__ctltasks = {}
        self.register_ctltask("host.list", self.ctl_host_list)


    def register_ctltask(self, method, func):
        """
        Register control method
        """
        self.__ctltasks[method] = func


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

    def handle_worker_join(self, msg):
        """
        New worker started
        """
        self.worker_start(msg['service'], msg['addr'], True )

    def handle_worker_leave(self, msg):
        """
        Worker is going down
        """
        self.worker_stop( msg['addr'], True )

    def handle_ping(self, msg):
        """
        Worker notify about himself
        """
        now = datetime.now()
        addr = msg['addr']
        svce = msg['service']
        try:
            png = self.__pingdb[addr]
        except KeyError:
            # service is already unknown
            self.worker_start(svce, addr, True)
            return

        if png['s']!=svce:
            # different service under same address!
            LOG.error("Service [%s] expected on address [%s], but [%s] appeared. Removing old, registering new." % (png['s'], addr, svce))
            self.worker_stop(addr, True)
            self.worker_start(svce, addr, True)
            return

        # update heartbeats
        png['t'] = now
        self.__pingdb[addr] = png


    def handle_name_query(self, msg):
        """
        Odpowiedź na pytanie o adres workera
        """
        name = msg['service']
        res = self.DB.get_worker_for_service(name)
        return {
            'message':messages.WORKER_ADDR,
            'service':name,
            'addr':res
        }

    def handle_control_request(self, msg):
        """
        wywołanie specjalne servicebus, nie workera.
        """
        return self.handle_inter_sync(msg, local=True)



    # worker state changes
    # ------------------------------

    def worker_start(self, service, address, local):
        """
        Handle joining to network by worker (local or blobal)
        """
        succ = self.DB.worker_register(service, address, local)
        if succ:
            if local:
                LOG.info("Local worker [%s] started, address [%s]" % (service, address) )
            else:
                LOG.info("Remote worker [%s] started, address [%s]" % (service, address) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            self.__pingdb[address] =  {
                's' : service,
                't' : datetime.now(),
            }
            # rozesłanie informacji w sieci
            self.BC.send_worker_start(service, address)


    def worker_stop(self, address, local):
        """
        Handle worker leaving network (local or global)
        """
        service = self.DB.worker_unregister(address)
        if service:
            if local:
                LOG.info("Local worker [%s] stopped, address [%s]" % (service, address) )
            else:
                LOG.info("Remote worker [%s] stopped, address [%s]" % (service, address) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            try:
                del self.__pingdb[address]
            except KeyError:
                pass
            self.BC.send_worker_stop(address)


    def request_workers_register(self):
        """
        Send to all local workers request for registering in network.
        Its used after new host start.
        """
        msg = {"message":messages.WORKER_REREG}
        for worker in self.DB.get_local_workers():
            sck = self.context.socket(zmq.REQ)
            try:
                sck.connect(worker)
                sck.send( serialize(msg) )
                res = sck.recv()
            finally:
                sck.close()


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
            # deregister all died workers
            while len(unreglist)>0:
                self.worker_stop(unreglist.pop(), True)

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



    def handle_inter_sync(self, message, local=False):
        """
        All control requests are realised here,
        """
        #print "CONTROL REQUEST, local:",local, "\n",message
        mthd = ".".join(message['method'])
        LOG.debug("Management call [%s]" % mthd)
        func = self.__ctltasks[mthd]
        result = func(message, local)
        return {"message":messages.RESULT, "result":result }


    def ctl_host_list(self, message, local):
        """
        List of all hosts in service bus
        """
        #import pprint
        #pprint.pprint( self.DB.services )
        lst = []
        for u,h,a in self.DB.host_list():
            lst.append( {'addr':a.rsplit(":",1)[0], 'uuid':u, 'hostname':h} )
            #for a,s in
            print "-"*40
            print h
            for a in self.DB.workers_on_host(u):
                print a
            #    print a,s

        return lst
