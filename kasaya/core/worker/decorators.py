#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .worker_reg import worker_methods_db
import inspect

__all__ = ("Task", "before_worker_start", "after_worker_stop")



def _func_only(func):
    """
    raises exception if parameter is not function
    """
    if inspect.isfunction(func):
        return
    else:
        raise Exception("Only functions can be tasks")



class Task(object):
    """
    Register in service
    """
    def __init__(self, name=None, timeout=None, anonymous=True, permissions=None):
        self.name = name
        self.timeout = timeout
        self.anonymous = anonymous
        self.permissions = permissions

    def __call__(self, func):
        _func_only(func)
        worker_methods_db.register_task(
            self.name, func, self.timeout, self.anonymous, self.permissions)
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

