#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from servicebus.worker.worker_base import WorkerBase
from servicebus.conf import settings
from servicebus.protocol import messages
from worker_reg import worker_methods_db
from servicebus.binder import bind_socket_to_port_range
from gevent_zeromq import zmq
import traceback
import gevent
from servicebus.lib.loops import RepLoop
from syncclient import SyncClient
import uuid


class WorkerDaemon(object):

    def __init__(self, servicename):
        self.proc_id = str(uuid.uuid4())
        self.servicename = servicename
        self.loop = RepLoop(self.connect)
        self.SYNC = SyncClient(servicename, self.address)
        # registering handlers
        self.loop.register_message( messages.SYNC_CALL, self.handle_sync_call )
        self.loop.register_message( messages.PING, self.handle_ping )
        self.loop.register_message( messages.WORKER_REREG, self.handle_request_register )
        self.loop.register_message( messages.CTL_CALL, self.handle_control_request )


    def connect(self, context):
        sock = context.socket(zmq.REP)
        self.address = bind_socket_to_port_range(sock, settings.WORKER_MIN_PORT, settings.WORKER_MAX_PORT)
        return sock


    def run(self):
        self.SYNC.notify_start()
        try:
            gevent.joinall([
                gevent.spawn(self.loop.loop),
            ])
        finally:
            self.loop.close()
            self.SYNC.notify_stop()
            self.SYNC.close()


    def stop(self):
        self.loop.stop()


    # --------------------


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


