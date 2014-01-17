#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
# monkey patching
from kasaya.core.lib.mpatcher import damonkey
damonkey()
del damonkey
# imports
from kasaya import Task, before_worker_start, after_worker_stop, after_worker_start, control
from kasaya.core.worker.decorators import raw_task, task
from kasaya.core.protocol import messages
from kasaya.conf import settings
from kasaya.core.protocol import Serializer
from kasaya.core.client.proxies import RawProxy
from kasaya.core import exceptions
from kasaya.core.client.worker_finder import WorkerFinder

from kasaya.core.lib.logger import LOG
LOG.stetupLogger("async")

import time
from pprint import pprint

#from kasaya.core.client.proxies import SyncProxy
#from async_backend import AsyncBackend
#from gevent import *
import gevent
from gevent.coros import Semaphore
#from gevent.coros import Semaphore
#from gevent.pool import Pool




class AsyncWorker(object):

    def __init__(self, worker_id):
        # used in process of selecting jobs
        self.own_async_id = worker_id
        # database setup
        dbb = settings.ASYNC_DB_BACKEND
        if dbb=="sqlite":
            from db.sqlite import SQLiteDatabase
            self.DB = SQLiteDatabase( worker_id )
        else:
            raise Exception("Unknown database backend defined in configuration: %r" % dbb)
        # serializer / deserializer
        self.serializer = Serializer()
        # caller
        self.PROXY = RawProxy()
        self._processing = True

        self.SEMA = Semaphore()


    def get_database_id(self):
        return self.DB.CONF['databaseid']

    def close(self):
        self._processing = False
        self.DB.close()


    # task processing loop

    def task_eater(self):
        rectime = time.time()
        while self._processing:
            taskproc = self.process_next_job()
            if taskproc:
                gevent.sleep()
            else:
                gevent.sleep(2)
            if rectime<time.time():
                g = gevent.Greenlet(self.check_lost_tasks)
                g.start()
                rectime = time.time() + settings.ASYNC_RECOVERY_TIME
                gevent.sleep()


    def start_eat(self):
        g = gevent.Greenlet(self.task_eater)
        g.start()


    # task scheduling


    def task_add_new(self, task, context, args, kwargs, ign_result=False):
        """
        Register task in database.
            task - task name with worker
            context - context data
            args, kwargs - function arguments
            ign_result - ignore result flag (True / False)
        """
        args = (args, kwargs)
        try:
            svc,task = task.split(".",1)
        except IndexError:
            raise Exception("wrong task name")
        taskid = self.DB.task_add(
            service = svc,
            taskname = task,
            time = time.time(),
            args = self.serializer.data_2_bin(args),
            context = self.serializer.data_2_bin(context),
            ign_result = ign_result,
        )
        return taskid


    def _flush_cache_for_service(self, servicename):
        """
        Flushes local worker list cache for service.
        It's used after worker down during task processing, to avoid sending
        next jobs to same worker again. Kasaya daemon should detect worker
        problem and will not send this worker address again.
        """
        WorkerFinder()._reset_cache(servicename)


    def process_task(self, taskid):
        """
        Process job with given ID
        """
        LOG.debug("Processing task %i" % taskid)
        # get task from database
        data = self.DB.task_start_process(taskid)
        if data is None:
            return
        # unpack data
        data['args'],data['kwargs'] = self.serializer.bin_2_data(data['args'])
        data['context'] = self.serializer.bin_2_data(data['context'])
        # send task to realize
        try:
            result = self.PROXY.sync_call(
                data['service'],
                data['task'],
                data['context'],
                data['args'],
                data['kwargs'],
            )

        except exceptions.SerializationError:
            # Exception raised when serialization or deserialization fails.
            # If this exception occurs here, we can't try to run this task again, because
            # data stored in async database are probably screwed up (for example async daemon
            # died or was stopped, but current configuration is different and uses other
            # serialization methods). We mark this task as permanently broken (it will be never repeated).
            self.DB.task_fail_permanently(taskid)
            return

        except exceptions.ServiceNotFound:
            # Worker not found occurs when destination service is currently not
            # available. Task will be repeated in future.
            self.DB.task_error_and_delay(taskid, settings.ASYNC_ERROR_TASK_DELAY)
            # mark all other tasks to same service as delayed,
            # to avoid spamming kasaya dameon
            self.DB.delay_tasks_for_service(data['service'], settings.ASYNC_ERROR_TASK_DELAY)
            return

        except exceptions.ServiceBusException:
            # Any other internal exception
            # will bump error counter
            self.DB.task_error_and_delay(taskid, settings.ASYNC_ERROR_TASK_DELAY)
            return

        except exceptions.NetworkError:
            # worker died, network fault or other low level networking error
            self.DB.task_error_and_delay(taskid, settings.ASYNC_ERROR_TASK_DELAY)
            self._flush_cache_for_service(data['service'])
            return

        except Exception as e:
            # other errors uncatched before
            self.DB.task_error_and_delay(taskid, settings.ASYNC_ERROR_TASK_DELAY)
            self._flush_cache_by_task_name(data['task'])
            return


        if result['message'] == messages.ERROR:
            # task prodoced error
            # not kasaya exception, but task's own error
            # it's not our fault ;-)

            # get task context or create it if not exist
            ctx = data['context']
            if ctx is None:
                ctx = {}

            # increace error count
            errcnt = ctx.get("err_count", 0) + 1
            ctx['err_count'] = errcnt

            # if error counter is limited, is that limit reached?
            maxerr = ctx.get("err_max", None)
            if not maxerr is None:
                no_retry = errcnt>=maxerr
            else:
                no_retry = False

            data['context'] = ctx
            if no_retry:
                self.DB.task_fail_permanently(
                    taskid,
                    settings.ASYNC_ERROR_TASK_DELAY,
                    self.serializer.data_2_bin(result) )
                self.DB.task_store_context(
                    taskid,
                    self.serializer.data_2_bin(ctx) )
            else:
                self.DB.task_error_and_delay(taskid,
                    settings.ASYNC_ERROR_TASK_DELAY,
                    self.serializer.data_2_bin(result) )
                self.DB.task_store_context(
                    taskid,
                    self.serializer.data_2_bin(ctx) )
            return

        # task is processed succesfully
        self.DB.task_finished_ok(taskid, self.serializer.data_2_bin( result ) )



    def process_next_job(self):
        """
        Check if is any job waiting and start processing it.
        """
        self.SEMA.acquire()
        try:
            taskid = self.DB.task_choose_for_process()
        finally:
            self.SEMA.release()

        if taskid is None:
            # nothing is waiting
            return False
        else:
            self.process_task( taskid )
            return True


    def check_lost_tasks(self):
        """
        Find tasks assigned to unexisting async workers and reassign them to self.
        once - if true, then not register task to do it again in future
        also:
        Check database for dead tasks: task waiting long with status=1
        (selected to process, but unprocessed)
        also:
        Find tasks with status=2 - processing, but without asyncid (after asyncid died).
        """
        # get all tasks belonging to dead async workers
        lost_asyncd = 0
        for asyncid in self.DB.async_list():
            if control.worker.exists( asyncid ):
                # worker exists
                continue
            # found lost async daemon tasks,
            # reassign them to self
            rc = self.DB.unlock_lost_tasks(asyncid)
            lost_asyncd =+ 1

        # process all tasks with status 1 -> selected for process but unprocessed long time
        self.DB.recover_unprocessed_tasks( settings.ASYNC_DEAD_TASK_TIME_LIMIT )

        # process all tasks with status 2 -> processing started, but async daemon died before receiving result
        self.DB.recover_unfinished_tasks()




# prepare

@before_worker_start
def setup_async(ID):
    global ASYNC
    ASYNC = AsyncWorker(ID)
    LOG.info( "Database id: %s" % ASYNC.get_database_id() )

@after_worker_start
def async_started():
    global ASYNC
    ASYNC.start_eat()

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
    res = ASYNC.task_add_new(
        msg['method'],
        msg['context'],
        msg['args'],
        msg['kwargs']
    )
    return res




# normal task

@task()
def get_task_result(task_id):
    global ASYNC
    pass





if __name__=="__main__":
    from kasaya import WorkerDaemon
    # If no service name is given, global and local settings will be used
    # skip_loading_modules flag means that worker will not load modules to
    # expose as service. All methods are actually defined in this file.
    daemon = WorkerDaemon(skip_loading_modules=True)
    daemon.run()


