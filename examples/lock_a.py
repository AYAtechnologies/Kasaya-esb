#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import Task, sync
from gevent import monkey
monkey.patch_all()
import gevent

@Task(name="task_a")
def task_a(param):
    print ("Starting A task")
    print ("calling B task")
    res = param+" a"
    res = sync.lockb.task_b(res)
    print ("B task result", res)
    print ("Finishing A task")
    return res



@Task(name="task_c")
def task_c(param):
    print ("Starting C task")
    res = param+" c"
    gevent.sleep(1)
    print ("Finishing C task")
    return res


@Task(name="burak")
def task_c():
    print ("Test buraka")
    #raise Exception("Task się wywalił :(")
    return "Udane"


if __name__=="__main__":
    from kasaya import WorkerDaemon
    #load_config_from_file("example.conf", optional=True)
    daemon = WorkerDaemon("locka")
    daemon.run()


