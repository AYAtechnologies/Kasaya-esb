#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import load_config_from_file
from kasaya.core.worker import Task

import time

@Task(name="print_foo")
def print_foo(param):
    print("print_foo", param)
    print()

@Task(name="do_work")
def do_work(a,b,foo=None, baz=None):
    print("do_work")
    print("params", a,b,foo,baz)
    print()
    return "hopsasa fikumiku rezultat"

@Task(name="another_task")
def fiku(a,b=None,foo=None, baz=None):
    print("another_task")


@Task(name="long_task")
def long_task(a, x):
    print (x, "sleeping:", a)
    time.sleep(float(a))
    return ("hurra " + str(x))

@Task(name="wrong")
def wrong_task(param):
    return param / 0


from kasaya.core.worker import WorkerDaemon


if __name__=="__main__":
    load_config_from_file("example.conf", optional=True)
    daemon = WorkerDaemon("myservice")
    daemon.run()


