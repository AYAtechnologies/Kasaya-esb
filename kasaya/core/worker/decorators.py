#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .worker_reg import worker_methods_db
import inspect


class Task(object):

    def __init__(self, name=None, timeout=None, anonymous=True, permissions=None):
        self.name = name
        self.timeout = timeout
        self.anonymous = anonymous
        self.permissions = permissions

    def __call__(self, func):
        if inspect.isfunction(func):
            worker_methods_db.register_task(
                self.name, func, self.timeout, self.anonymous, self.permissions)
        else:
            raise Exception("Only functions can be tasks")
        return func
