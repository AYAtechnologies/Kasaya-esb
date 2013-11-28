#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.protocol import Serializer, messages
from kasaya.core import SingletonCreator
from kasaya.core.exceptions import ReponseTimeout
from gevent.coros import Semaphore
from kasaya.core.protocol.comm import Sender, ConnectionClosed
import gevent
from gevent.coros import Semaphore
from kasaya.core.lib import LOG

#, SingletonCreator
class KasayaLocalClient(Sender):
    """
    KasayaLocalClient is communication class to talk with kasaya daemon.
    It's used by workers and clients.
    """

    __metaclass__ = SingletonCreator

    def __init__(self, *args, **kwargs):
        # connect to kasaya
        super(KasayaLocalClient, self).__init__('tcp://127.0.0.1:'+str(settings.KASAYAD_CONTROL_PORT), *args, **kwargs)
        self.SEMA = Semaphore()

    # worker methods

    def setup(self, servicename, address, ID, pid):
        self.srvname = servicename
        self.ID = ID
        self.__pingmsg = {
            "message" : messages.WORKER_LIVE,
            "addr" : address,
            "id" : self.ID,
            "service" : servicename,
            "pid": pid,
            "status": 0,
        }

    def notify_worker_live(self, status):
        self.SEMA.acquire()
        self.__pingmsg['status'] = status
        try:
            self.send(self.__pingmsg)
            return True
        except ConnectionClosed:
            return False
        finally:
            self.SEMA.release()

    def notify_worker_stop(self):
        self.SEMA.acquire()
        msg = {
            "message" : messages.WORKER_LEAVE,
            "id" : self.ID,
            }
        try:
            self.send(msg)
            return True
        except ConnectionClosed:
            return False
        finally:
            self.SEMA.release()


    # client methods

    def query(self, service):
        """
        odpytuje lokalny serwer kasaya o to gdzie realizowany
        jest serwis o żądanej nazwie
        """
        msg = {'message':messages.QUERY, 'service':service}
        self.SEMA.acquire()
        try:
            return self.send_and_receive(msg)
        finally:
            self.SEMA.release()


    def control_task(self, msg):
        """
        zadanie tego typu jest wysyłane do serwera kasayad nie do workera!
        """
        self.SEMA.acquire()
        try:
            return self.send_and_receive(msg)
        finally:
            self.SEMA.release()

