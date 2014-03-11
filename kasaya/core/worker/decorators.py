#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from .worker_reg import worker_methods_db
import inspect

__all__ = ("Task", "task", "before_worker_start", "after_worker_start", "after_worker_stop")



def _func_only(func):
    """
    raises exception if parameter is not function
    """
    if inspect.isfunction(func):
        return
    else:
        raise Exception("Only functions can be tasks")



class task(object):
    """
    Register task in service
    """
    def __init__(self, name=None,
                 timeout=None,
                 permissions=None,
                 retry_limit=0,
                 delay_time=3,
                 close_django_conn=None,
                ):
        """
        name - task name if different than function name
        timeout - maximum task execution time in seconds
        permissions - permissions required to run this task (unsupported)
        retry_limit - how many times this task can be automatically repeated in case of exception during task processing
        delay_time - if task will be executed again after exception, how many seconds should we wait before re-run
        close_dj_conn - when working with django ORM set this flag to true to close
                     db connection after fucntion ends.
        """
        self.name = name
        self.timeout = timeout
        self.permissions = permissions
        self.retry_limit = retry_limit
        if close_django_conn is None:
            lose_django_conn = settings.DJANGO_ORM_CLOSE_CONN_AFTER_TASKS
        self.close_dj_conn = close_django_conn

    def __call__(self, func):
        _func_only(func)
        worker_methods_db.register_task(
            self.name, func, self.timeout, self.permissions, self.close_dj_conn)
        return func


# in future 'Task' will be removed, use 'task' instead
Task = task




class raw_task(object):
    """
    Special internal purpose task decorator for use in own kasaya daemons
    """
    def __init__(self, message_type, raw_responce=False):
        #super (internal_task, self).__init__(*args, **kwargs)
        self.message_type = message_type
        self.raw_responce = raw_responce

    def __call__(self, func):
        _func_only(func)
        worker_methods_db.register_raw_task(
            self.message_type, self.raw_responce, func)
        return func




def before_worker_start(func):
    """
    Register function to be executed just before worker start
    """
    _func_only(func)
    worker_methods_db.register_before_start(func)
    return func


def after_worker_start(func):
    """
    Registers function to be executed after worker start listening for jobs
    """
    _func_only(func)
    worker_methods_db.register_after_start(func)
    return func


def after_worker_stop(func):
    """
    Register function to be executed after worker is stopped
    """
    _func_only(func)
    worker_methods_db.register_after_stop(func)
    return func

