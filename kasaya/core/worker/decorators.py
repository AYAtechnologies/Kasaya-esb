#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .worker_reg import worker_methods_db
import inspect


class Task(object):

    def __init__(self, name=None, timeout=None):
        self.name = name
        self.timeout = timeout

    def __call__(self, func):
        if inspect.isfunction(func):
            worker_methods_db.register_task(self.name, func, self.timeout)
        else:
            raise Exception("Only functions can be tasks")
        return func
