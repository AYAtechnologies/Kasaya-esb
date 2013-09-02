#!/usr/bin/env python
#coding: utf-8
from worker_reg import worker_methods_db


class Task(object):

    def __init__(self, name=None):
        self.name = name

    def __call__(self, func):
        worker_methods_db.register_method(self.name, func)

        return func # nie dekorujemy

#        def wrap(request, *args, **kwargs):
#            return func(request, *args, **kwargs)
#
#        return wrap
