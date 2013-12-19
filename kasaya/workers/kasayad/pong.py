#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.events import emit
import time
import gevent


class PingDB(object):
    """
    emits two events:
        worker-local-start - when new local worker started
        worker-local-stop - when local work stopped working
    """

    def __init__(self):
        self._pingdb = {}


    def ping(self, worker_id):
        """
        Podbija czas ostatniego pinga dla podanego workera
        """
        try:
            dat = self._pingdb[worker_id]
        except KeyError:
            return
        dat['t'] = time.time()
        self._pingdb[worker_id] = dat


    def ping_ex(self, worker_id, service, addr, pid):
        """
        Sprawdza inne parametry workera podczas pinga i w razie potrzeby
        przerejestrowuje workera.
        """
        try:
            dat = self._pingdb[worker_id]
        except KeyError:
            dat = None

        if dat is None:
            dat = {}
            dat['s'] = service
            dat['a'] = addr
            dat['p'] = pid
            # deferred on_worker_start notification
            emit("worker-local-start", worker_id, addr, service, pid )
        else:
            if not ( (dat['s']==service) and (dat['a']==addr) ):
                # it's impossible that worker with same ID has other
                # service name or address. So we unregister existing worker.
                # New one will be registered on next heartbeat loop.
                emit("worker-local-stop", worker_id )
                del self._pingdb[worker_id]
                return

        dat['t'] = time.time()
        self._pingdb[worker_id] = dat



    def check_all(self):
        """
        Checks last activity of workers and unregister the dead ones.
        """
        t = time.time()
        maxdiff = settings.WORKER_HEARTBEAT * 2.5
        for worker_id, dat in self._pingdb.items():
            if (t-dat['t'])>maxdiff:
                emit("worker-local-stop", worker_id )
                del self._pingdb[worker_id]



    def loop(self):
        """
        Periodically check all locally registered workers ping time. Unregister dead workers
        """
        while True:
            self.check_all()
            gevent.sleep(settings.WORKER_HEARTBEAT)
