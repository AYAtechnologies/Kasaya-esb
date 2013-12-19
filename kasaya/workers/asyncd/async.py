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

from kasaya.core.lib.logger import LOG
LOG.stetupLogger("async")

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

    def get_database_id(self):
        return self.DB.CONF['databaseid']

    def close(self):
        self.DB.close()

    def task_add_new(self, task, context, args, kwargs, ign_result=False):
        """
        Register task in database.
            task - task name with worker
            context - context data
            args, kwargs - function arguments
            ign_result - ignore result flag (True / False)
        """
        args = (args, kwargs)
        taskid = self.DB.task_add(
            taskname = task,
            time = time.time(),
            args = self.serializer.data_2_bin(args),
            context = self.serializer.data_2_bin(context),
            ign_result = ign_result,
        )
        self._check_next()
        return taskid


    def process_task(self, taskid):
        """
        Process job with given ID
        """
        LOG.debug("Processing task %i" % taskid)
        # get task from database
        data = self.DB.task_get_to_process(taskid)
        if data is None:
            return
        # unpack data
        data['args'],data['kwargs'] = self.serializer.bin_2_data(data['args'])
        data['context'] = self.serializer.bin_2_data(data['context'])
        # send task to realize
        try:
            result = self.PROXY.sync_call(
                data['task'],
                data['context'],
                data['args'],
                data['kwargs'],
            )

        except exceptions.SerializationError:
            """
            Exception raised when serialization or deserialization fails.
            If this exception occurs here, we can't try to run this task again, because
            data stored in async database are probably screwed up (for example async daemon
            died or was stopped, but current configuration is different and uses other
            serialization methods). We mark this task as permanently broken (and newer will be repeated).
            """
            self.DB.task_fail_permanently(taskid)

        except exceptions.ServiceNotFound:
            """
            Worker not found occurs when destination service is currently not
            available. Task will be repeated in future.
            """
            self.DB.task_error_and_delay(taskid, settings.ASYNC_ERROR_TASK_DELAY)
            return

        # task is processed succesfully
        self.DB.task_finished_ok(taskid, self.serializer.data_2_bin( result ) )

        # and finally...
        self._check_next()


    def process_next_job(self):
        """
        Check if there is waiting job to do,
        and start processing if is available
        """
        taskid = self.DB.task_get_next_id()
        if taskid is None:
            # nothing to do, check for lost tasks
            grnlt = gevent.Greenlet(self._check_dead_tasks)
            grnlt.start()
            return
        else:
            # task is waiting
            self.process_task( taskid )


    def _check_dead_tasks(self):
        """
        Check database for dead tasks:
        - task waiting long with status=1 (selected to process, but unprocessed for long time)
        """
        dt = self.DB.task_find_dead( settings.ASYNC_DEAD_TASK_TIME_LIMIT )
        if dt is None:
            return
        self.DB.task_unselect_to_process(dt)
        self._check_next()

    def _check_lost_tasks(self):
        """
        Find tasks assigned to unexisting async workers and reassign them to self
        """
        for asyncid in self.DB.async_list():
            if control.worker.exists( asyncid ):
                # worker exists
                continue
            print ("LOST ASYNC", asyncid)
            rc = self.DB.take_over_tasks(asyncid)
            print (rc)
        self._check_dead_tasks()


    def _check_next(self):
        """
        After each function which adds or removes task from queue
        this function should be called.
        """
        grnlt = gevent.Greenlet(self.process_next_job)
        grnlt.start()




# prepare

@before_worker_start
def setup_async(ID):
    global ASYNC
    ASYNC = AsyncWorker(ID)
    LOG.info( "Database id: %s" % ASYNC.get_database_id() )

@after_worker_start
def async_started():
    global ASYNC
    ASYNC._check_lost_tasks()
    ASYNC._check_dead_tasks()
    ASYNC._check_next()

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


