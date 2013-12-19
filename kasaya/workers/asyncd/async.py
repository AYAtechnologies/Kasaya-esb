#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
# monkey patching
from kasaya.core.lib.mpatcher import damonkey
damonkey()
del damonkey
# imports
from kasaya import Task, before_worker_start, after_worker_stop, after_worker_start
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
        # caller
        self.PROXY = RawProxy()

    def get_database_id(self):
        return self.DB.CONF['databaseid']

    def close(self):
        self.DB.close()

    def task_add_new(self, task, context, args, kwargs):
        """
        Register task in database
        """
        args = (args, kwargs)
        taskid = self.DB.task_add(
            taskname = task,
            time = time.time(),
            args = self.serializer.data_2_bin(args),
            context = self.serializer.data_2_bin(context)
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
        except exceptions.ServiceNotFound:
            self.DB.task_error_and_delay(taskid, settings.ASYNC_ERROR_TASK_DELAY)
            return
        #except Exception as e:


        print (result)


    def next_job(self):
        """
        Check if there is waiting job to do,
        and start processing if is available
        """
        taskid = self.DB.task_get_next_id()
        if taskid is None: return
        self.process_task( taskid )

    def _check_next(self):
        """
        After each function which adds or removes task from queue
        this function should be called.
        """
        grnlt = gevent.Greenlet(self.next_job)
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


