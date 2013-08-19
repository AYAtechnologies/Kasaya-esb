#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )

from servicebus.conf import load_config_from_file
load_config_from_file("../config.txt")

from backend import DictBackend as Backend
from servicebus.worker import WorkerDaemon
from servicebus.worker.decorators import Task
from servicebus.client.task_caller import find_worker
from servicebus.protocol import serialize, deserialize, messages
from servicebus.client.task_caller import execute_sync_task

import uuid

class AsyncDeamon(WorkerDaemon):

    def __init__(self):
        super(AsyncDeamon, self).__init__("async_daemon")
        self.backend = Backend()

    def loop(self):
        while True:
            msgdata = self.WORKER.recv()
            msgdata = deserialize(msgdata)
            msg = msgdata['message']
            if msg == messages.SYNC_CALL:
                # żądanie wykonania zadania
                name = msgdata['service']
                result = self.run_task(
                        funcname = msgdata['method'],
                        args = self + msgdata['args'],
                        kwargs = msgdata['kwargs']
                )

            elif msg == messages.ASYNC_CALL:
                result = self.run_task(
                        funcname = msgdata['method'],
                        args= [self],
                        kwargs = msgdata,
                )
            else:
                # zawsze trzeba odpowiedzieć na zapytanie
                result = ""
                #result = func(*args, **kwargs)
            self.WORKER.send(serialize(result))

    def execute_async(self, task_id, method, authinfo, args, kwargs):
        res = execute_sync_task(method, authinfo, 0, args, kwargs)
        print res

    @Task(name="register")
    def register(self, *args, **kwargs):
        print "register:", args, kwargs
        task_id = self.backend.add_task(kwargs)
        self.execute_async(task_id, kwargs["original_method"], kwargs["authinfo"], kwargs["args"], kwargs["kwargs"])
        # msg ={
        # "message": messages.RESULT,
        # "result" : task_id
        # }
        return task_id

    @Task(name="get_result")
    def get_result(self, *args, **kwargs):
        print "get result:", args, kwargs
        return self.backend.get_result()





