#!/usr/bin/env python
#coding: utf-8
from servicebus.worker.worker_base import WorkerBase
from servicebus.protocol import messages
from worker_reg import worker_methods_db
import traceback


class WorkerDaemon(WorkerBase):

    def __init__(self, servicename):
        super(WorkerDaemon, self).__init__(servicename)
        self.register_message( messages.SYNC_CALL, self.handle_sync_call )
        self.register_message( messages.PING, self.handle_ping )


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


    def run_task(self, funcname, args, kwargs):
        funcname = ".".join( funcname )
        # find task in worker db
        print self.servicename, ":", funcname
        try:
            func = worker_methods_db[funcname]
        except KeyError:
            # we dont know tish task
            return {
                'message' : messages.ERROR,
                'internal' : True,  # internal service bus problem
                'info' : 'Method %s not found' % funcname,
            }
        # try to run function and catch exceptions
        try:
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


