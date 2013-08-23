#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )

from servicebus.conf import settings
from backend import DictBackend as Backend
from servicebus.worker import WorkerDaemon
from servicebus.worker.decorators import Task
from servicebus.protocol import serialize, deserialize, messages
from servicebus.client.task_caller import execute_sync_task, find_worker

from gevent import *
from gevent.coros import Semaphore


class AsyncDeamon(WorkerDaemon):

    def __init__(self):
        super(AsyncDeamon, self).__init__(settings.ASYNC_DAEMON_SERVICE)
        self.register_message(messages.ASYNC_CALL, self.handle_async_call)
        self.backend = Backend()
        self.greenlets_semaphore = Semaphore()
        self.greenlets = {}
        self.exposed_methods = ["register_task", "get_task_result"]


    def handle_async_call(self, msgdata):
        result = self.run_task(
            funcname = msgdata['method'],
            args = [],  # <<< ??????????
            kwargs = msgdata,
        )
        return result


    def handle_async_result(self, greenlet):
        self.greenlets_semaphore.acquire()
        task_id = self.greenlets[greenlet]
        self.greenlets_semaphore.release()
        if greenlet.successful():
            self.backend.set_result_success(task_id, greenlet.value)
        else:
            self.backend.set_result_fail(task_id, "error", greenlet.exception)
            # ponawiać ?
        #print self.backend.store

    def register_task(self, *args, **kwargs):
        print "register:", args, kwargs
        task = {}
        task["method"] = kwargs["original_method"]
        task["authinfo"] = kwargs["authinfo"]
        task["args"] = kwargs["args"]
        task["kwargs"] = kwargs["kwargs"]
        task_id = self.backend.add_task(task)
        worker = find_worker(kwargs["original_method"])
        self.backend.start_execution(task_id, self.proc_id, worker)
        g = Greenlet(execute_sync_task, kwargs["original_method"], kwargs["authinfo"], 0, kwargs["args"], kwargs["kwargs"])
        self.greenlets_semaphore.acquire()
        self.greenlets[g] = task_id
        self.greenlets_semaphore.release()
        g.link(self.handle_async_result)
        g.start()
        return task_id

    def get_task_result(self, task_id):
        print "get result:", task_id
        return self.backend.get_result(task_id)

    #@Task(name="kill")
    #def get_result(self, task_id):
    #    print "get result:", task_id
    #    return self.backend.get_result(task_id)





