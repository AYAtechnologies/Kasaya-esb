#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from servicebus.binder import bind_socket_to_port_range
from gevent_zeromq import zmq
import gevent
from servicebus.protocol import serialize, deserialize, messages
from syncclient import SyncClient



class WorkerDaemon(object):

    def __init__(self, servicename):
        self.context = zmq.Context()
        self.WORKER = self.context.socket(zmq.REP)
        self.address = bind_socket_to_port_range(self.WORKER, settings.WORKER_MIN_PORT, settings.WORKER_MAX_PORT)
        # syncd client
        self.servicename = servicename
        self.SYNC = SyncClient(servicename, self.address)


    def loop(self):
        while True:
            msgdata = self.WORKER.recv()
            msgdata = deserialize(msgdata)
            print "<<<",msgdata
            msg = msgdata['message']

            if msg==messages.SYNC_CALL:
                # żądanie wykonania zadania
                name = msgdata['service']
                reply = {'fikuku':"jajeczko"}
                self.WORKER.send( serialize(reply) )
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                self.WORKER.send("")


    def run(self):
        self.SYNC.notify_start()
        try:
            gevent.joinall([
                gevent.spawn(self.loop),
            ])
        finally:
            self.WORKER.close()
            self.SYNC.notify_stop()
            self.SYNC.close()


