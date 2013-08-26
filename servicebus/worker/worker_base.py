#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.conf import settings
from servicebus.binder import bind_socket_to_port_range
from gevent_zeromq import zmq
import gevent
from syncclient import SyncClient
from servicebus.protocol import serialize, deserialize, messages
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
        self.__running = True


    def register_message(self, message, func):
        self._msgdb[message]=func


    def loop(self):
        while self.__running:
            msgdata = self.WORKER.recv()
            msgdata = deserialize(msgdata)

            try:
                m = msgdata['message']
                handler = self._msgdb[ m ]
            except KeyError:
                # unknown messages are ignored silently
                self.WORKER.send("")
                continue
            result = handler(msgdata)
            if result is None:
                result = {"message": messages.NOOP}
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


    def stop(self):
        """
        Request warm stop of worker.
        After finishing current task event loop will exit.
        """
        self.__running = False
