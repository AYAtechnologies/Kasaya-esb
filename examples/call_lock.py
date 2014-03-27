#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from kasaya.conf import load_config_from_file
import sys
import gevent.monkey; gevent.monkey.patch_thread()
from kasaya import sync,Context


if __name__=="__main__":
    #try:
    #    print C.sync.kasatest_a.test_empty(1)
    #except Exception as e:
    #    print e.traceback

    #try:
    #CC = Context()
    #s = CC.sync
    #print CC.sync.kasatest_a.aa.bb("A")#test_empty("A")

    cc = Context()
    cc['test'] = "jajeczko"
    with cc as C:
        #try:
        #    print "RESULT:",C.sync.kasatest_a.test_subrequests("A")
        #except Exception as e:
        #    print "EXCEPTION"
        #    print e.traceback

        try:
            C.sync.kasatest_a.test_infinite_loop(0)
        except Exception as e:
            #print type(e)
            print( e.info() )
        #pass

    #sync.kasatest_a.test_infinite_loop(0)
    sync.kasatest_a.test_exception(10)
    #sync.kasatest_a.test_subrequests("A")

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