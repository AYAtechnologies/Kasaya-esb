#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core.lib.binder import bind_socket_to_port_range
from kasaya.core.protocol import messages
from kasaya.core.lib.comm import RepLoop, send_and_receive, exception_serialize_internal, exception_serialize
from kasaya.core.middleware.core import MiddlewareCore
from kasaya.core.lib.control_tasks import ControlTasks
from kasaya.core.lib import LOG, system
from kasaya.core import exceptions
from .worker_reg import worker_methods_db
from .syncclient import SyncClient
import zmq.green as zmq
import gevent
import gevent.monkey

import traceback
import datetime, uuid, os
import inspect

__all__=("WorkerDaemon",)


class TaskTimeout(Exception): pass


class WorkerDaemon(MiddlewareCore):

    def __init__(self, servicename):
        self.uuid = str(uuid.uuid4())
        self.servicename = servicename
        LOG.info("Starting worker daemon, service [%s], uuid: [%s]" % (self.servicename, self.uuid) )
        self.loop = RepLoop(self.connect)
        LOG.debug("Connected to socket [%s]" % (self.loop.address) )
        self.SYNC = SyncClient(servicename, self.loop.ip, self.loop.port, self.uuid, os.getpid())
        # registering handlers
        self.loop.register_message( messages.SYNC_CALL, self.handle_sync_call, raw_msg_response=True )
        self.loop.register_message( messages.CTL_CALL, self.handle_control_request )
        # heartbeat
        self.__hbloop=True
        #exposing methods
        self.exposed_methods = []
        MiddlewareCore.__init__(self)
        # control tasks
        self.ctl = ControlTasks( self.loop.get_context() )
        self.ctl.register_task("stop", self.CTL_stop )
        self.ctl.register_task("stats", self.CTL_stats )
        self.ctl.register_task("tasks", self.CTL_methods )
        # stats
        #self._sb_errors = 0 # internal service bus errors
        self._tasks_succes = 0 # succesfully processed tasks
        self._tasks_error = 0 # task which triggered exceptions
        self._tasks_nonex = 0 # non existing tasks called
        self._tasks_control = 0 # control tasks received
        self._start_time = datetime.datetime.now() # time of worker start

        if settings.WORKER_MONKEY:
            gevent.monkey.patch_all()


    def connect(self, context):
        sock = context.socket(zmq.REP)
        addr = bind_socket_to_port_range(sock, settings.WORKER_MIN_PORT, settings.WORKER_MAX_PORT)
        return sock, addr

    def run(self):
        LOG.debug("Sending notification to local sync daemon. Service [%s] starting on address [%s]" % (self.servicename, self.loop.address))
        self.SYNC.notify_live()
        self.setup_middleware()
        self.__greens = []
        self.__greens.append( gevent.spawn(self.loop.loop) )
        self.__greens.append( gevent.spawn(self.heartbeat_loop) )
        try:
            gevent.joinall(self.__greens)
        finally:
            self.stop()
            self.close()

    def stop(self):
        self.__hbloop = False
        LOG.debug("Sending stop notification. Address [%s]" % self.loop.address)
        self.SYNC.notify_stop()
        self.loop.stop()
        # killing greenlets
        for g in self.__greens:
            g.kill(block=True)
        LOG.debug("Worker [%s] stopped" % self.servicename)

    def close(self):
        self.loop.close()
        self.SYNC.close()



    # --------------------
    # Hearbeat

    def heartbeat_loop(self):
        while self.__hbloop:
            self.SYNC.notify_live()
            gevent.sleep(settings.WORKER_HEARTBEAT)


    # --------------------


#    def expose_method(self, method_name):
#        if getattr(self, method_name):
#            # this must be a defined method - attribute error raises when trying to expose method that doesnt exist
#            self.exposed_methods.append(method_name)
#
#    def expose_all(self):
#        exposed = []
#        for name, val in inspect.getmembers(self):
#            if inspect.ismethod(val) and not name.startswith("_"):
#                exposed.append(name)
#        self.exposed_methods += exposed


    def handle_sync_call(self, msgdata):
        name = msgdata['service']
        msgdata = self.prepare_message(msgdata)
        result = self.run_task(
            funcname = msgdata['method'],
            args = msgdata['args'],
            kwargs = msgdata['kwargs']
        )
        result = self.postprocess_message(result)
        return result


    def handle_control_request(self, message):
        self._tasks_control += 1
        result = self.ctl.handle_request(message)
        return result
        #return {"message":messages.RESULT, "result":result }


    def run_task(self, funcname, args, kwargs):
        funcname = ".".join( funcname )

        # find task in worker db
        #self_func = getattr(self, funcname, None)
        #if self_func is not None and funcname in self.exposed_methods:
        #    func = self_func
        #    # doesnt work - dont know why
        #    # if funcname in worker_methods_db:
        #    #     print "Warning ", funcname, "in self.exposed_methods and in worker_functions"
        #else:
        try:
            task = worker_methods_db[funcname]
        except KeyError:
            self._tasks_nonex += 1
            LOG.info("Unknown worker task called [%s]" % funcname)
            return exception_serialize_internal( 'Method %s not found' % funcname )

        # try to run function and catch exceptions
        try:
            func = task['func']
            tout = task['timeout']
            if tout is None:
                # call task without timeout
                result = func(*args, **kwargs)
            else:
                # call task with timeout
                with gevent.Timeout(tout, TaskTimeout):
                    result = func(*args, **kwargs)
            self._tasks_succes += 1
            task['res_succ'] += 1

            return {
                'message' : messages.RESULT,
                'result' : result
            }

        except TaskTimeout as e:
            # timeout exceeded
            self._tasks_error += 1
            task['res_tout'] += 1
            err = exception_serialize(e, internal=False)
            LOG.info("Task [%s] timeout (after %i s)." % (funcname, tout) )
            return err

        except Exception as e:
            # exception occured
            self._tasks_error += 1
            task['res_err'] += 1
            err = exception_serialize(e, internal=False)
            LOG.info("Task [%s] exception [%s]. Message: %s" % (funcname, err['name'], err['description']) )
            LOG.debug(err['traceback'])
            return err


    # worker internal control tasks
    # -----------------------------


    def CTL_stop(self):
        """
        Stop request. Finish current task and shutdown.
        """
        g = gevent.Greenlet(self.stop)
        g.start_later(2)
        return True

    def CTL_stats(self):
        """
        Return current worker stats
        """
        now = datetime.datetime.now()
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