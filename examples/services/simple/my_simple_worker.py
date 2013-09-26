#!/usr/bin/env python
#coding: utf-8
from kasaya import Task
import time

@Task(name="print_foo")
def print_foo(param):
    print "print_foo", param

@Task(name="do_work")
def do_work(a,b,foo=None, baz=None):
    print "Hi! Im important task which does nothing."
    print "my params:",a,b,foo,baz
    return "some result"

@Task(name="another_task")
def i_do_another_task(a,b=None,foo=None, baz=None):
    print "another_task"

@Task(name="long_task")
def long_task(a, x):
    print "sleeping:", a, "...",
    time.sleep(float(a))
    return "done: " + str(x)

@Task(name="wrong")
def wrong_task(param):
    print "I will raise exception"
    return param / 0

