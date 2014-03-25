#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import Task, sync



@Task()
def test_empty(param):
    pass


@Task(name="aa.bb")
def test_empty2(param):
    pass

# embedded calls
@Task()
def test_subrequests(param):
    return sync.kasatest_b.subcall_1( param )
@Task()
def subcall_2(param):
    return sync.kasatest_b.subcall_3( param+"C" )
@Task()
def subcall_3(param):
    return sync.kasatest_b.subcall_5( param+"E" )


@Task()
def test_infinite_loop(num):
    return sync.kasatest_b.test_infinite_loop(num+1)



if __name__=="__main__":
    from kasaya import WorkerDaemon
    #load_config_from_file("example.conf", optional=True)
    daemon = WorkerDaemon("kasatest_a")
    daemon.run()


