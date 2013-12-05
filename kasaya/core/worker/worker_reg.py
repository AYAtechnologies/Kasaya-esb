#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.lib import LOG


class WorkerMethodsDB(object):

    def __init__(self):
        self.db = {}
        self._before_start = []
        self._after_stop = []

    def register_task(self, name, func, timeout, anonymous, permissions):
        # task name
        if name is None:
            name = func.__name__
        if name in self.db:
            if func==self.db[name]['func']:
                return
            c = "Task %s is already registered" % name
            LOG.critical(c)
            raise Exception(c)

        # timeout
        if not timeout is None:
            if type(timeout)!=int:
                raise Exception("Timeout must be integer value")
            if timeout<0:
                raise Exception("Timeout cannot be negative value")

        # extra params for task
        doc = func.__doc__
        if not doc is None:
            doc = doc.strip()
        taskdata = {
            'func' : func,
            'doc' : doc, # docstring
            'timeout' : timeout, # timeout in seconds
            'anon' : anonymous, # can task be executed without authorisation
            'perms' : permissions, # permissions required to call task
            'res_succ' : 0, # successful calls
            'res_err' : 0,  # error finishing calls
            'res_tout' : 0, # timed out calls
        }

        self.db[name] = taskdata
        LOG.debug("Registered task %s" % name)

    def __getitem__(self, name):
        return self.db[name]

    def tasks(self):
        return self.db.keys()


    # before / after worker running functions

    def register_before_start(self, func):
        self._before_start.append( func )

    def register_after_stop(self, func):
        self._after_stop.append( func )



worker_methods_db = WorkerMethodsDB()

