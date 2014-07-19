#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
# more kasaya imports
from kasaya.conf import settings
from kasaya.core.protocol import messages
from kasaya.core.worker.worker_base import WorkerBase, TaskExecutor
from kasaya.core.protocol.comm import MessageLoop
from kasaya.core.lib.control_tasks import ControlTasks
from kasaya.core.lib import LOG, system
from kasaya.core.protocol.kasayad_client import KasayaLocalClient
from kasaya.core import exceptions
from .worker_reg import worker_methods_db

#from kasaya.core.lib import django_integration as DJI

import traceback
import time, os
from kasaya.core.lib.system import monkey
from kasaya.core.lib import LOG

__all__=("WorkerDaemon",)



class TaskTimeout(Exception): pass

#from kasaya.core.events import OnEvent
#@OnEvent("sender-conn-reconn")
#def mesydzek(addr):
#    LOG.debug ("Trying to reconnect with %s" % addr)



class WorkerDaemon(WorkerBase, TaskExecutor):

    def __init__(self,
            servicename=None,
            load_config=True,
            skip_loading_modules=False):

        # gevent import and patching
        monkey()
        global gevent, add_event_handler
        import gevent
        from kasaya.core.events import add_event_handler

        # config loader
        if servicename is None:
            load_config = True
        if load_config:
            LOG.info("Loading service.conf")
            if servicename is None:
                servicename = self.__load_config()

        super(WorkerDaemon, self).__init__(servicename)

        self.__skip_loading_modules = skip_loading_modules

        # worker status
        # 0 - initialized
        # 1 - starting or waiting for reconnect to kasaya
        # 2 - working
        # 3 - stopping
        # 4 - dead
        self.status = 0
        LOG.info("Starting worker daemon, service [%s], ID: [%s]" % (self.servicename, self.ID) )
        adr = "tcp://%s:%i" % (settings.BIND_WORKER_TO, settings.WORKER_MIN_PORT)
        self.loop = MessageLoop(adr, settings.WORKER_MAX_PORT)

        add_event_handler( "sender-conn-closed", self.kasaya_connection_broken )
        add_event_handler( "sender-conn-started", self.kasaya_connection_started )

        self.SYNC = KasayaLocalClient( sessionid=self.ID )
        self.SYNC.setup( servicename, self.loop.address, self.ID, os.getpid() )
        LOG.debug("Binded to socket [%s]" % (",".join(self.loop.binded_ip_list()) ) )

        # registering handlers
        self.loop.register_message( messages.SYNC_CALL, self.handle_task_request, raw_msg_response=True )
        self.loop.register_message( messages.CTL_CALL, self.handle_control_request )
        # heartbeat
        self.__hbloop=True
        #exposing methods
        self.exposed_methods = []
        # control tasks
        self.ctl = ControlTasks()
        self.ctl.register_task("stop", self.CTL_stop )
        self.ctl.register_task("start", self.CTL_start )
        self.ctl.register_task("stats", self.CTL_stats )
        self.ctl.register_task("tasks", self.CTL_methods )
        # stats
        #self._sb_errors = 0 # internal service bus errors
        self._tasks_succes = 0 # succesfully processed tasks
        self._tasks_error = 0 # task which triggered exceptions
        self._tasks_nonex = 0 # non existing tasks called
        self._tasks_control = 0 # control tasks received
        self._start_time = time.time() # time of worker start


    def __load_config(self):
        """
        This function is used only if servicename is not given, and
        daemon is not started by kasaya daemon.
        """
        from kasaya.conf import load_worker_settings, set_value
        try:
            config = load_worker_settings("service.conf")
        except IOError:
            import sys
            LOG.critical("File 'service.conf' not found, unable to start service.")
            sys.exit(1)

        # system settings overwriting
        for k,v in config['config'].items():
            set_value(k, v)

        # worker environment
        for k,v in config['env'].items():
            os.environ[k.upper()] = v

        # service name
        svcc = config['service']
        svname = svcc['name']
        LOG.info("Service config loaded. Service name: %s" % svname)

        # set flag to load tasks automatically
        self.__auto_load_tasks_module = svcc['module']

        return svname

    def __load_modules(self):
        try:
            modname = self.__auto_load_tasks_module
        except AttributeError:
            return
        __import__(modname)


    def _worker_just_started(self):
        """
        Called before worker starts serving tasks.
        It's called before worker connect's to kasaya daemon.
        """
        # register raw message handlers...
        for msg, fdata in worker_methods_db._raw_tasks.items():
            self.loop.register_message( msg, fdata['func'], raw_msg_response=fdata['raw_resp'] )

        # run functions before worker start
        for func in worker_methods_db._before_start:
            func(self.ID)


    def _worker_listening(self):
        """
        Called after worker start listening regular jobs
        """
        # run functions after worker start
        for func in worker_methods_db._after_start:
            func()


    def _worker_just_stopped(self):
        """
        After worker is shutted down, we cal this.
        This function is called after closing connection with kasaya daemon.
        """
        for func in worker_methods_db._after_stop:
            func()


    def run(self):
        if not self.__skip_loading_modules:
            self.__load_modules()
        self.status = 1
        LOG.debug("Service [%s] starting." % self.servicename)
        # before run...
        self._worker_just_started()
        self.__greens = []
        self.__greens.append( gevent.spawn(self.loop.loop) )
        self.__greens.append( gevent.spawn(self.heartbeat_loop) )
        try:
            gevent.joinall(self.__greens)
        finally:
            self.stop()
            self.close()
            # just finished working
            self._worker_just_stopped()

    def stop(self):
        self.__hbloop = False
        self.status = 3
        LOG.debug("Sending stop notification. Address [%s]" % self.loop.address)
        self.SYNC.notify_worker_stop()
        self.loop.stop()
        # killing greenlets
        for g in self.__greens:
            g.kill(block=True)
        LOG.debug("Worker [%s] stopped" % self.servicename)

    def close(self):
        self.loop.close()
        self.SYNC.close()
        self.status=4


    # network state changed

    def kasaya_connection_broken(self, addr):
        """
        Should be called when connection with kasaya is broken.
        """
        LOG.debug("Connection closed with %s", addr)
        if self.status<3: # is worker is already stopping?
            self.status = 1 #set status as 1 - waiting for start


    def kasaya_connection_started(self, addr):
        """
        This will be called when connection with kasaya is started
        """
        LOG.debug("Connected to %s", addr)
        self.SYNC.notify_worker_live(self.status)




    # --------------------
    # Hearbeat

    def heartbeat_loop(self):
        failmode = False
        while self.__hbloop:
            res = self.SYNC.notify_worker_live(self.status)
            gevent.sleep(settings.WORKER_HEARTBEAT)


    # --------------------


    def handle_control_request(self, addr, message):
        self._tasks_control += 1
        result = self.ctl.handle_request(message)
        return result


    def _find_task(self, taskname):
        return worker_methods_db[taskname]


    # worker internal control tasks
    # -----------------------------


    def CTL_stop(self):
        """
        Stop request. Finish current task and shutdown.
        """
        self.status = 3
        g = gevent.Greenlet(self.stop)
        g.start_later(2)
        return True

    def CTL_start(self):
        """
        Set status of worker as running. This allow to process tasks
        """
        if self.status==1:
            self.status = 2
            LOG.info("Received status: running.")
            # call tasks after worker started listening
            g = gevent.Greenlet( self._worker_listening )
            g.start()
            return True
        return False


    def CTL_stats(self):
        """
        Return current worker stats
        """
        now = time.time()
        uptime = now - self._start_time
        uptime = uptime.seconds # temporary workaround
        return {
            "task_succ" : self._tasks_succes,
            "task_err"  : self._tasks_error,
            "task_nonx" : self._tasks_nonex,
            "task_ctl"  : self._tasks_control,
            "ip"        : self.loop.ip,
            "port"      : self.loop.port,
            "service"   : self.servicename,
            "mem_total" : system.get_memory_used(),
            "uptime"    : uptime,
        }

    def CTL_methods(self):
        """
        List all exposed methods by worker
        """
        lst = []
        tasks = worker_methods_db.tasks()
        tasks.sort()
        for name in tasks:
            nfo = worker_methods_db[name]
            res = {
                'name' : name,
                'doc' : nfo['doc'],
                'timeout' : nfo['timeout'],
                'anonymous' : nfo['anon'],
                'permissions' : nfo['perms']
            }
            lst.append(res)
        return lst


    def CTL_middleware_list(self, midlist):
        """
        Register middleware list for worker
        """
        pass
