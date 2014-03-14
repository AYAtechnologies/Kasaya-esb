#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import Task, sync


@Task()
def test_empty():
    pass

@Task()
def subcall_1(param):
    return sync.kasatest_a.subcall_2( param+"B" )



if __name__=="__main__":
    from kasaya import WorkerDaemon
    #load_config_from_file("example.conf", optional=True)
    daemon = WorkerDaemon("kasatest_b")
    daemon.run()


