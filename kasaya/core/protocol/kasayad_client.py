#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol.comm import SimpleSender, ConnectionClosed
from kasaya.core.protocol import messages
from kasaya.core import SingletonCreator
from kasaya.core import exceptions
from kasaya.conf import settings
from kasaya.core.lib import LOG
from random import choice
from time import time



#, SingletonCreator
class KasayaLocalClient(SimpleSender):
    """
    KasayaLocalClient it is low level interface to communicate with kasaya daemon.
    It's used by workers and clients.
    """

    __metaclass__ = SingletonCreator

    def __init__(self, *args, **kwargs):
        # connect to kasaya
        super(KasayaLocalClient, self).__init__('tcp://127.0.0.1:'+str(settings.KASAYAD_CONTROL_PORT), *args, **kwargs)

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
        self.__pingmsg['status'] = status
        try:
            self.send(self.__pingmsg)
            return True
        except ConnectionClosed:
            return False

    def notify_worker_stop(self):
        msg = {
            "message" : messages.WORKER_LEAVE,
            "id" : self.ID,
            }
        try:
            self.send(msg)
            return True
        except ConnectionClosed:
            return False


    # client methods

    def query(self, service):
        """
        odpytuje lokalny serwer kasaya o to gdzie realizowany
        jest serwis o żądanej nazwie
        """
        msg = {'message':messages.QUERY, 'service':service}
        return self.send_and_receive(msg)


    def query_multi(self, service):
        """
        Get all active workers realising given service
        """
        msg = {'message':messages.QUERY_MULTI, 'service':service}
        return self.send_and_receive(msg)


    def control_task(self, msg):
        """
        zadanie tego typu jest wysyłane do serwera kasayad nie do workera!
        """
        return self.send_and_receive(msg)




class WorkerFinder(object):
    __metaclass__ = SingletonCreator

    def __init__(self, allow_caching=False):
        self._allow_caching = False
        self._kasaya = KasayaLocalClient()
        if allow_caching:
            self.enable_cache()


    def enable_cache(self):
        """
        Enable worker base caching
        """
        if self._allow_caching:
            return
        self._allow_caching = True
        self._cache = {}


    def disable_cache(self):
        """
        Disable worker base caching
        """
        if not self._allow_caching:
            return
        self._allow_caching = False
        try:
            del self._cache
        except AttributeError:
            pass


    def _reset_cache(self, servicename):
        """
        Flush cache with workes list for specified servicename
        """
        if self._allow_caching:
            try:
                del self._cache[servicename]
            except KeyError:
                pass


    def find_worker(self, service_name, tag=None):
        """
        tag - additional attribute of worker, used to filter workers by data set, or other attribute
        """
        if not tag is None:
            raise NotImplemented("tagging workers is not supported yet")

        if not self._allow_caching:
            # single request
            msg = self._kasaya.query( service_name )
            if not msg['message']==messages.WORKER_ADDR:
                raise exceptions.ServiceBusException("Wrong response from sync server")
            addr = msg['addr']
            if addr is None:
                raise exceptions.ServiceNotFound("No service '%s' found" % service_name)
            return addr

        # cached request
        try:
            # random choice from available workers
            wdata = self._cache[ service_name ]
            if wdata['t'] <= time():
                return choice( wdata['w'] )
            else:
                # cache data is outdated
                self._reset_cache(service_name)
        except KeyError:
            pass

        # no cached workers, ask kasaya for available workers.
        msg = self._kasaya.query_multi( service_name )
        if not msg['message']==messages.WORKER_ADDR:
            raise exceptions.ServiceBusException("Wrong response from sync server")

        # no result
        if msg['addrlst'] is None:
            raise exceptions.ServiceNotFound("No service '%s' found" % service_name)

        res = {
            'w':msg['addrlst'],
            't':time() + msg['timeout']
        }
        self._cache[service_name] = res

        return choice( res['w'] )




