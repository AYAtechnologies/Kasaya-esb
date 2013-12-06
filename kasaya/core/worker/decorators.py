#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .worker_reg import worker_methods_db
import inspect

__all__ = ("Task", "task", "before_worker_start", "after_worker_stop")



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
    def __init__(self, name=None, timeout=None, anonymous=True, permissions=None):
        """
        name - task name if different than function name
        timeout - maximum task execution time in seconds
        anonymous - this task can be called anonymous (unsuppoerted)
        permissions - permissions required to run this task (unsupported)
        """
        self.name = name
        self.timeout = timeout
        self.anonymous = anonymous
        self.permissions = permissions

    def __call__(self, func):
        _func_only(func)
        worker_methods_db.register_task(
            self.name, func, self.timeout, self.anonymous, self.permissions)
        return func

# in future Task will be replaced with task for consistency with other decorators
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



def after_worker_stop(func):
    """
    Register function to be executed after worker is stopped
    """
    _func_only(func)
    worker_methods_db.register_after_stop(func)
    return func

