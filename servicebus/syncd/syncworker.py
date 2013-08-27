#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.conf import settings
from servicebus import exceptions
from servicebus.protocol import serialize, deserialize, messages
from servicebus.binder import get_bind_address
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

    def __init__(self, server):
        self.SRV = server
        self.context = zmq.Context()
        self.__pingdb = {}

        self.queries = RepLoop(self._connect_queries_loop, context=self.context)
        self.queries.register_message(messages.WORKER_JOIN,  self.handle_worker_join)
        self.queries.register_message(messages.WORKER_LEAVE, self.handle_worker_leave)
        self.queries.register_message(messages.PING, self.handle_ping)
        self.queries.register_message(messages.QUERY,  self.handle_name_query)
        self.queries.register_message(messages.CTL_CALL, self.handle_control_request)


    def _connect_queries_loop(self, context):
        """
        connect local queries loop
        """
        sock = context.socket(zmq.REP)
        addr = 'ipc://'+settings.SOCK_QUERIES
        sock.bind(addr)
        LOG.debug("Connected query socket %s" % addr)
        return sock, addr

    def _connect_syncd_dialog_loop(self, context):
        """
        connext global network syncd dialog
        """
        sock = context.socket(zmq.REP)
        addr = get_bind_address(settings.SYNCD_CONTROL_BIND)
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


    # starting loops

    def get_loops(self):
        return [self.queries.loop]#, self.hearbeat_loop]

    # message handlers
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
        res = self.SRV.DB.get_worker_for_service(name)
        return {
            'message':messages.WORKER_ADDR,
            'service':name,
            'addr':res
        }

    def handle_control_request(self, msg):
        """
        wywołanie specjalne servicebus, nie workera
        """
        print "CONTROL REQUEST"
        print msg



    # worker state changes
    # ------------------------------

    def worker_start(self, service, address, local):
        """
        Handle joining to network by worker (local or blobal)
        """
        succ = self.SRV.DB.worker_register(service, address, local)
        if succ:
            if local:
                LOG.info("Local worker [%s] started, address %s" % (service, address) )
            else:
                LOG.info("Remote worker [%s] started, address %s" % (service, address) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            self.__pingdb[address] =  {
                's' : service,
                't' : datetime.now(),
            }
            # rozesłanie informacji w sieci
            self.SRV.BC.send_worker_start(service, address)


    def worker_stop(self, address, local):
        """
        Handle worker leaving network (local or global)
        """
        succ = self.SRV.DB.worker_unregister(address)
        if succ:
            if local:
                LOG.info("Local worker [%s] stopped" % (service, address) )
            else:
                LOG.info("Remote worker [%s] stopped" % (service, address) )
        if local:
            # dodanie serwisu do bazy z czasami pingów
            try:
                del self.__pingdb[address]
            except KeyError:
                pass
            self.SRV.BC.send_worker_stop(address)


    def request_workers_register(self):
        """
        Send to all local workers request for registering in network.
        Its used after new host start.
        """
        msg = {"message":messages.WORKER_REREG}
        for worker in self.SRV.DB.get_local_workers():
            sck = self.context.socket(zmq.REQ)
            try:
                sck.connect(worker)
                sck.send( serialize(msg) )
                res = sck.recv()
            finally:
                sck.close()




    '''
    # heartbeat
    def hearbeat_loop(self):
        pinglife = timedelta( seconds = settings.HEARTBEAT_TIMEOUT )

        while True:
            msg = {"message":messages.PING}
            lworkers = self.SRV.DB.get_local_workers()
            for worker in lworkers:
                now = datetime.now()

                # is last heartbeat fresh enough?
                lhb = self.SRV.DB.get_last_worker_heartbeat(worker)
                delta = now-lhb
                if delta<=pinglife:
                    continue

                # ping with timeout
                try:
                    with gevent.Timeout(settings.PING_TIMEOUT, TooLong):
                        pingres = self.ping_worker(worker)
                    if pingres:
                        self.SRV.DB.set_last_worker_heartbeat(worker, now)
                        continue
                except TooLong:
                    self.PINGER.close()
                    self.PINGER = None

                # worker died
                print "worker", worker, "died or broken"
                self.SRV.DB.worker_unregister(worker)
                msg = {"message":messages.WORKER_LEAVE, "addr":worker}
                self.SRV.BC.broadcast_message(msg)

            gevent.sleep(settings.WORKER_HEARTBEAT)



    def ping_worker(self, addr):
        #context = zmq.Context()
        self.PINGER = self.context.socket(zmq.REQ)
        self.PINGER.connect(addr)
        # send ping
        msg = {"message":messages.PING}
        self.PINGER.send( serialize(msg) )
        # result of ping
        try:
            res = self.PINGER.recv()
            self.PINGER.close()
            res = deserialize(res)
            if res["message"] != messages.PONG:
                return False
        except:
            return False
        return True

    '''
