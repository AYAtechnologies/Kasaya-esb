#!/usr/bin/env python
#coding: utf-8
import settings
import zmq
#from protocol import serialize, deserialize


class WorkerLoop(object):

    def __init__(self):
        pass

    def run(self):
        pass


#
#try:
#    import random
#    addr = "1.2.3.4:"+str(random.randint(5000,6000))
#    nsc = SyncClient("fikumiku", addr)
#    nsc.notify_start()
#    import time
#    time.sleep(60)
#finally:
#    nsc.notify_stop()
#