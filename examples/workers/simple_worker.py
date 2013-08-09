#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
print esbpath
sys.path.append( esbpath )


from servicebus.worker.decorators import Task


@Task()
def print_foo(param):
    print "param", param



from servicebus.worker.syncclient import SyncClient

try:
    import random
    addr = "1.2.3.4:"+str(random.randint(5000,6000))
    nsc = SyncClient("fikumiku", addr)
    nsc.notify_start()
    import time
    time.sleep(60)
finally:
    nsc.notify_stop()


