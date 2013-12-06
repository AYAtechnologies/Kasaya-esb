#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya import Task, before_worker_start, after_worker_stop
from kasaya.core.worker.decorators import raw_task
from kasaya.core.protocol import messages
from kasaya.conf import settings
from kasaya.core.protocol import Serializer

import time
from pprint import pprint

#from kasaya.core.client.proxies import SyncProxy
#from async_backend import AsyncBackend
#from gevent import *
import gevent
#from gevent.coros import Semaphore
#from gevent.pool import Pool



class AsyncWorker(object):

    def __init__(self, worker_id):
        # used in process of selecting jobs
        #self.worker_id = worker_id
        # database setup
        dbb = settings.ASYNC_DB_BACKEND
        if dbb=="sqlite":
            from db.sqlite import SQLiteDatabase
            self.DB = SQLiteDatabase( worker_id )
        else:
            raise Exception("Unknown database backend defined in configuration: %r" % dbb)
        # serializer / deserializer
        self.serializer = Serializer()

    def close(self):
        self.DB.close()

    def register_task(self, task, context, args, kwargs):
        #bin_2_data()
        args = (args, kwargs)
        taskid = self.DB.task_add(
            taskname = task,
            time = time.time(),
            args = self.serializer.data_2_bin(args),
            context = self.serializer.data_2_bin(context)
        )
        return taskid

    def next_job(self):
        #gevent.Greenlet()
        self.DB.task_get_next()

    def check(self):
        self.next_job()




# prepare

@before_worker_start
def setup_async(ID):
    global ASYNC
    ASYNC = AsyncWorker(ID)

@after_worker_stop
def stop_async():
    global ASYNC
    ASYNC.close()



# catch tasks

@raw_task(messages.ASYNC_CALL)
def add_task_to_queue(msg):
    """
    Catch new task and store in database
    """
    global ASYNC
    #pprint(msg)
    #res = ASYNC.register_task(
    #    msg['method'],
    #    msg['context'],
    #    msg['args'],
    #    msg['kwargs']
    #)
    res="11"
    ASYNC.check()
    return res




# normal task

@Task()
def get_task_result(task_id):
    global ASYNC
    pass




'''
class AsyncWorkerOld(object):


    def __init__(self):
        super(AsyncDeamon, self).__init__(settings.ASYNC_DAEMON_SERVICE)
        self.loop.register_message(messages.SYSTEM_CALL, self.handle_async_call)
        self.backend = AsyncBackend("name")
        self.greenlets_semaphore = Semaphore()
        self.greenlets = {}
        self.pool = Pool(size=settings.WORKER_POOL_SIZE)
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
        #print self.backend.backend

    def sanitize_loose_task(self, task_id):
        task = self.backend.get_task(task_id)
        #register_async_task(task["method"], task["authinfo"], 0, task["args"], task["kwargs"])
        if task["status"] == self.backend.TASK_WAITING:
            print "SANITIZING", task
            self.execute_task(task_id, task["method"], task["context"], task["args"], task["kwargs"])
        else:
            print "TASK ERROR:", task

    def execute_task(self, task_id, method, context, args, kwargs):
        s = SyncProxy()
        s.initialize(method, context)
        worker = s.addr
        self.backend.start_execution(task_id, worker)
        return s(*args, **kwargs)

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
'''




if __name__=="__main__":
    from kasaya import WorkerDaemon
    # If no service name is given, global and local settings will be used
    # skip_loading_modules flag means that worker will not load modules to
    # expose as service. All methods are actually defined in this file.
    daemon = WorkerDaemon(skip_loading_modules=True)
    daemon.run()


