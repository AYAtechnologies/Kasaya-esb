#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )


from servicebus.conf import load_config_from_file

import time

from servicebus.worker.decorators import Task

@Task(name="print_foo")
def print_foo(param):
    print "print_foo", param
    print

@Task(name="do_work")
def do_work(a,b,foo=None, baz=None):
    print "do_work"
    print "params", a,b,foo,baz
    print
    return "hopsasa fikumiku rezultat"

@Task(name="another_task")
def fiku(a,b=None,foo=None, baz=None):
    print "another_task"
    print

@Task(name="messages.mail.send_heavy_spam")
def sendspam(a,b):
    print "sendspam"
    print a,b

@Task(name="long_task")
def long_task(a, x):
    print x, "sleeping:", a
    time.sleep(float(a))
    return "hurra " + str(x)


@Task(name="wyjebka")
def wyjebka(param):
    return param / 0


from servicebus.worker import Daemon
import middleware.logging.worker as logging

if __name__=="__main__":
    load_config_from_file("../config.txt")
    demon = Daemon("fikumiku")
    demon.middleware_classes = [ logging.WorkerMiddleware]
    demon.run()




#from servicebus.worker.syncclient import SyncClient


#try:
#    import random
#    addr = "1.2.3.4:"+str(random.randint(5000,6000))
#    nsc = SyncClient("fikumiku", addr)
#    nsc.notify_start()
#    import time
#    time.sleep(60)
#finally:
#    nsc.notify_stop()

