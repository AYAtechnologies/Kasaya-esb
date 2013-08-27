#!/usr/bin/env python
#coding: utf-8
import sys
import os

esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )

from servicebus.conf import settings
from servicebus.backend import RedisBackend as Backend
# from backend import DictBackend as Backend
from servicebus.worker import WorkerDaemon
from servicebus.protocol import messages
from servicebus.client.task_caller import execute_sync_task, find_worker

from gevent import *
from gevent.coros import Semaphore
from gevent.pool import Pool

class AsyncDeamon(WorkerDaemon):
    def __init__(self):
        super(AsyncDeamon, self).__init__(settings.ASYNC_DAEMON_SERVICE)
        self.loop.register_message(messages.SYSTEM_CALL, self.handle_async_call)
        print "running at:", str(self.address)
        self.backend = Backend(self.proc_id, self.address)
        self.greenlets_semaphore = Semaphore()
        self.greenlets = {}
        self.pool = Pool(size=10)
        self.exposed_methods = ["register_task", "get_task_result"]


    def handle_async_call(self, msgdata):
        result = self.run_task(
            funcname = msgdata['method'],
            args = [],
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

    def sanitize_loose_task(self, task_id):
        task = self.backend.get_task(task_id)
        #register_async_task(task["method"], task["authinfo"], 0, task["args"], task["kwargs"])
        if task["status"] == self.backend.TASK_WAITING:
            print "SANITIZING", task
            self.execute_task(task_id, task["method"], task["context"], task["args"], task["kwargs"])
        else:
            print "TASK ERROR:", task

    def execute_task(self, task_id, method, context, args, kwargs):
        worker = find_worker(method)
        self.backend.start_execution(task_id, worker)
        return execute_sync_task(method, context, args, kwargs)

    def start_greenlet(self, g, task_id):
        self.greenlets_semaphore.acquire()
        self.greenlets[g] = task_id
        self.greenlets_semaphore.release()
        g.link(self.handle_async_result)
        self.pool.start(g)

    def register_task(self, *args, **kwargs):
        print "register:", args, kwargs
        task = {}
        task["method"] = kwargs["original_method"]
        task["context"] = kwargs["context"]
        task["args"] = kwargs["args"]
        task["kwargs"] = kwargs["kwargs"]
        task_id, loose_tasks = self.backend.add_task(task)
        g = Greenlet(self.execute_task, task_id, kwargs["original_method"], kwargs["context"], kwargs["args"], kwargs["kwargs"])
        self.start_greenlet(g, task_id)
        for t in loose_tasks:
            g = Greenlet(self.sanitize_loose_task, t)
            self.start_greenlet(g, t)
        return task_id

    def get_task_result(self, task_id):
        print "get result:", task_id
        return self.backend.get_result(task_id)






