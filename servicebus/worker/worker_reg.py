#!/usr/bin/env python
#coding: utf-8

class WorkerMethodsDB(object):

    def __init__(self):
        self.db = {}

    def register_method(self, name, func):
        # print "method registered:", name, " - " ,func
        self.db[name] = func

    def __getitem__(self, name):
        return self.db[name]


worker_methods_db = WorkerMethodsDB()

