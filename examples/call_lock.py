#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import sync


if __name__=="__main__":
    #load_config_from_file("example.conf", optional=True)
    #res = sync.locka.jajo.foo.baar.task_a("start!")
    #res = sync.locka.task_a("start!")
    for a in xrange(2):
        print "res", sync.locka.burak()
#import socket
#print socket.socket