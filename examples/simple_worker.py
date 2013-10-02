#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import load_config_from_file
from kasaya import Task


import time


@Task(name="print_foo")
def print_foo(param):
    """
    This function does foo
    """
    print("print_foo", param)
    print()

@Task(name="do_work")
def do_work(a,b,foo=None, baz=None):
    """
    Function which realises work
    """
    print("do_work")
    print("params", a,b,foo,baz)
    print()
    return "hopsasa fikumiku rezultat"

@Task(name="another_task")
def fiku(a,b=None,foo=None, baz=None):
    #"""
    #Fiku is very complicated math func
    #"""
    print("another_task")


@Task(name="long_task", timeout=1)
def long_task(a):
    """
    I'm sleepy...
    """
    print ("sleeping:", a)
    time.sleep(float(a))
    return ("waked up after "+str(a))


@Task(name="wrong")
def wrong_task(param):
    """
    Never divide by 0, until You really want exception.
    """
    return param / 0


if __name__=="__main__":
    from kasaya import WorkerDaemon
    load_config_from_file("example.conf", optional=True)
    daemon = WorkerDaemon("myservice")
    daemon.run()


