#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import sync,Context


if __name__=="__main__":
    #try:
    #    print C.sync.kasatest_a.test_empty(1)
    #except Exception as e:
    #    print e.traceback

    #try:
    with Context() as C:
        C.sync.kasatest_a.test_subrequests("A")
    #except Exception as e:
    #    print e#.traceback

    #load_config_from_file("example.conf", optional=True)
    #res = sync.locka.jajo.foo.baar.task_a("start!")
    #res = sync.locka.task_a("start!")
    #for a in xrange(2):
    #    print "res",
    #    try:
    #        print sync.locka.burak()
    #    except Exception as e:
    #        print e.traceback
    #        break

#import socket
#print socket.socket