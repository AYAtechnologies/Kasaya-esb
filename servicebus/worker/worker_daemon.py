#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.conf import settings
from servicebus.protocol import messages
from worker_reg import worker_methods_db
from servicebus.binder import bind_socket_to_port_range
from gevent_zeromq import zmq
import traceback
import gevent, gevent.threadpool
from servicebus.lib.loops import RepLoop
from syncclient import SyncClient
import uuid
import inspect


__all__=("WorkerDaemon",)


#import gevent.monkey;
#gevent.monkey.patch_all()



class WorkerDaemon(object):

    def __init__(self, servicename):
        self.proc_id = str(uuid.uuid4())
        self.servicename = servicename
        self.loop = RepLoop(self.connect)
        self.SYNC = SyncClient(servicename, self.loop.address)
        # registering handlers
        self.loop.register_message( messages.SYNC_CALL, self.handle_sync_call )
        #self.loop.register_message( messages.PING, self.handle_ping )
        self.loop.register_message( messages.WORKER_REREG, self.handle_request_register )
        self.loop.register_message( messages.CTL_CALL, self.handle_control_request )
        # heartbeat
        self.__hbloop=True


    def connect(self, context):
        sock = context.socket(zmq.REP)
        addr = bind_socket_to_port_range(sock, settings.WORKER_MIN_PORT, settings.WORKER_MAX_PORT)
        return sock, addr


    def run(self):
        self.SYNC.notify_start()
        # runs worker heartbeat and main loop in separeted threads
        # to avoid heartbeat disruption when doing heavy worker tasks
        #pool = gevent.threadpool.ThreadPool(2)
        wloop = gevent.spawn(self.loop.loop)
        hbeat = gevent.spawn(self.heartbeat_loop)
        try:
            gevent.joinall([
                wloop,
                hbeat,
            ])
        finally:
            self.loop.close()
            self.SYNC.notify_stop()
            self.SYNC.close()


    def stop(self):
        self.loop.stop()
        self.__hbloop = False


    # --------------------
    # Hearbeat

    def heartbeat_loop(self):
        import sys
        msg = {
            "message" : messages.PING,
            "addr" : self.loop.address,
            "service":self.servicename
        }
        while self.__hbloop:
            self.SYNC.send_raw(msg)
            gevent.sleep(settings.WORKER_HEARTBEAT)


    # --------------------


    def expose_method(self, method_name):
        if getattr(self, method_name):
            # this must be a defined method - attribute error raises when trying to expose method that doesnt exist
            self.exposed_methods.append(method_name)

    def expose_all(self):
        exposed = []
        for name, val in inspect.getmembers(self):
            if inspect.ismethod(val) and not name.startswith("_"):
                exposed.append(name)
        self.exposed_methods += exposed

    def handle_sync_call(self, msgdata):
        name = msgdata['service']
        result = self.run_task(
            funcname = msgdata['method'],
            args = msgdata['args'],
            kwargs = msgdata['kwargs']
        )
        return result


    def handle_ping(self, message):
        return {"message":messages.PONG}


    def handle_request_register(self, message):
        self.SYNC.notify_start()
        return {"message":messages.NOOP}


    def handle_control_request(self, message):
        method = ".".join( message['method'] )
        print "WORKER CONTROL REQUEST", method
        #if message['method']=="stop_all_workers":
        #    self.stop()


    def run_task(self, funcname, args, kwargs):
        funcname = ".".join( funcname )
        # find task in worker db
        print self.servicename, ":", funcname
        self_func = getattr(self, funcname, None)
        if self_func is not None and funcname in self.exposed_methods:
            func = self_func
        else:
            try:
                func = worker_methods_db[funcname]
            except KeyError:
                # we dont know this task
                return {
                    'message' : messages.ERROR,
                    'internal' : True,  # internal service bus problem
                    'info' : 'Method %s not found' % funcname,
                }
        # try to run function and catch exceptions
        try:
            #print ">  ",func
            #print ">  ",args
            #print ">  ",kwargs
            result = func(*args, **kwargs)
        except Exception as e:
            msg = {
                'message' : messages.ERROR,
                'internal' : False, # means that error is not internal bus error
                'traceback' : traceback.format_exc(),
                'info' : e.message,
                }
            return msg
        # normal function return
        return {
            'message' : messages.RESULT,
            'result' : result
        }


