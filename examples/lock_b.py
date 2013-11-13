#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import Task, sync
from gevent import monkey
monkey.patch_all()


@Task(name="task_b")
def task_b(param):
    print ("Starting B task")
    print ("calling C task")
    res = param+" b"
    res = sync.locka.task_c(res)
    #print ("B task result", res)
    print ("Finishing B task")
    return res


if __name__=="__main__":
    from kasaya import WorkerDaemon
    #load_config_from_file("example.conf", optional=True)
    daemon = WorkerDaemon("lockb")
    daemon.run()


