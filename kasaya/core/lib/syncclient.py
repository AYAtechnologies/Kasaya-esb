#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.protocol import Serializer, messages
from kasaya.core.exceptions import ReponseTimeout
from gevent.coros import Semaphore
#import zmq.green as zmq
import gevent
from kasaya.core.lib.comm import Sender


class KasayaLocalClient(Sender):
    """
    KasayaLocalClient is communication class to talk with kasaya daemon.
    It's used by workers and clients.
    """

    def __init__(self, servicename, ip, port, uuid, pid):
        self.srvname = servicename
        self.__addr = ip
        self.__port = port
        self.__pingmsg = {
            "message" : messages.WORKER_LIVE,
            "addr" : ip,
            "port" : port,
            "uuid" : uuid,
            "service" : servicename,
            "pid": pid,
            "status": 0,
        }
        # connect to zmq
        super(KasayaLocalClient, self).__init__('tcp://127.0.0.1:'+str(settings.KASAYAD_CONTROL_PORT) )


    def connect(self):
        #self.sync_sender = self.ctx.socket(zmq.REQ)
        ##self.sync_sender.setsockopt(zmq.LINGER, 1000) # two seconds
        ##self.sync_sender.setsockopt(zmq.HWM, 8) # how many messages buffer
        #self.sync_sender.connect( 'ipc://'+settings.SOCK_QUERIES )
        pass

    def disconnect(self):
        #self.sync_sender.disconnect(self.__sockaddr)
        #self.sync_sender.close()
        #del self.sync_sender
        #del self.ctx
        pass


    # worker methods

    def notify_live(self, status):
        self.__pingmsg['status'] = status
        return self.send(self.__pingmsg)

    def notify_stop(self):
        msg = {
            "message" : messages.WORKER_LEAVE,
            "ip" : self.__addr,
            "port" : self.__port,
            }
        self.send(msg)


    # client methods

    def query(self, service):
        """
        odpytuje lokalny serwer kasaya o to gdzie realizowany
        jest serwis o żądanej nazwie
        """
        msg = {'message':messages.QUERY, 'service':service}
        return send_and_receive(self.addr, msg)

    def control_task(self, msg):
        """
        zadanie tego typu jest wysyłane do serwera kasayad nie do workera!
        """
        return send_and_receive(self.addr, msg)
        #self.queries.send( serialize(msg) )
        #res = self.queries.recv()
        #res = deserialize(res)
        #return res

