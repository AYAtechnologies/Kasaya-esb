#!/usr/bin/env python
#coding: utf-8
from servicebus.conf import settings
from servicebus.binder import bind_socket_to_port_range
from gevent_zeromq import zmq
import gevent
from servicebus.protocol import serialize, deserialize, messages
from syncclient import SyncClient
from worker_reg import worker_methods_db
import traceback



class WorkerDaemon(object):

    def __init__(self, servicename):
        self.context = zmq.Context()
        self.WORKER = self.context.socket(zmq.REP)
        self.address = bind_socket_to_port_range(self.WORKER, settings.WORKER_MIN_PORT, settings.WORKER_MAX_PORT)
        # syncd client
        self.servicename = servicename
        self.SYNC = SyncClient(servicename, self.address)


    def loop(self):
        while True:
            msgdata = self.WORKER.recv()
            msgdata = deserialize(msgdata)
            msg = msgdata['message']

            if msg==messages.SYNC_CALL:
                # żądanie wykonania zadania
                name = msgdata['service']
                result = self.run_task(
                        funcname = msgdata['method'],
                        args = msgdata['args'],
                        kwargs = msgdata['kwargs']
                )
                self.WORKER.send( serialize(result) )
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                self.WORKER.send("")
                result = func(*args, **kwargs)


    def run_task(self, funcname, args, kwargs):
        funcname = ".".join( funcname )
        # find task in worker db
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


    def run(self):
        self.SYNC.notify_start()
        try:
            gevent.joinall([
                gevent.spawn(self.loop),
            ])
        finally:
            self.WORKER.close()
            self.SYNC.notify_stop()
            self.SYNC.close()


