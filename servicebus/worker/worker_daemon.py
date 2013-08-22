#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.worker.worker_base import WorkerBase
from servicebus.protocol import messages
from worker_reg import worker_methods_db
import traceback


class WorkerDaemon(WorkerBase):

    def __init__(self, servicename):
        super(WorkerDaemon, self).__init__(servicename)
        self.register_message( messages.SYNC_CALL, self.handle_sync_call )
        self.register_message( messages.PING, self.handle_ping )
        self.register_message( messages.WORKER_REREG, self.handle_request_register )
        self.register_message( messages.CTL_CALL, self.handle_control_request )


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

