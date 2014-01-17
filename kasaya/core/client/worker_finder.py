#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.lib.syncclient import KasayaLocalClient
from kasaya.core.protocol import messages
from kasaya.core import SingletonCreator
from kasaya.core import exceptions
from random import choice
from time import time



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




