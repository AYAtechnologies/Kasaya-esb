#!/usr/bin/env python
#coding: utf-8
from kasaya import Task, before_worker_start, after_worker_stop
#from kasaya.conf import settings
#from kasaya.core.protocol import messages
#from kasaya.core.client.proxies import SyncProxy
#from async_backend import AsyncBackend
#from gevent import *
#from gevent.coros import Semaphore
#from gevent.pool import Pool



class AsyncWorker(object):

    def __init__(self):
        pass

    def close(self):
        pass



@before_worker_start
def setup_async():
    global ASYNC
    ASYNC = AsyncWorker()

@after_worker_stop
def stop_async():
    global ASYNC
    ASYNC.close()


@Task()
def add_task_to_queue(task, context, args, kwargs):
    global ASYNC
    pass

@Task()
def get_task_result(task_id):
    global ASYNC
    pass



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





if __name__=="__main__":
    from kasaya import WorkerDaemon
    daemon = WorkerDaemon("async")
    daemon.run()


