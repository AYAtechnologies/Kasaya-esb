#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from servicebus.binder import bind_socket_to_port_range
from gevent_zeromq import zmq
import gevent
from syncclient import SyncClient
from servicebus.protocol import serialize, deserialize
import uuid


class WorkerBase(object):


    def __init__(self, servicename):
        # message handlers
        self._msgdb = {}
        self.proc_id = str(uuid.uuid4())
        # worker ZMQ
        self.context = zmq.Context()
        self.WORKER = self.context.socket(zmq.REP)
        self.address = bind_socket_to_port_range(self.WORKER, settings.WORKER_MIN_PORT, settings.WORKER_MAX_PORT)
        # syncd client
        self.servicename = servicename
        self.SYNC = SyncClient(servicename, self.address)


    def register_message(self, message, func):
        self._msgdb[message]=func


    def loop(self):
        while True:
            msgdata = self.WORKER.recv()
            msgdata = deserialize(msgdata)

            try:
                handler = self._msgdb[ msgdata['message'] ]
            except KeyError:
                # unknown messages are ignored silently
                self.WORKER.send("")
                continue
            result = handler(msgdata)
            self.WORKER.send( serialize(result) )


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


