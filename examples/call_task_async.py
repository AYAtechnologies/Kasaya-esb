#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

#from kasaya.conf import load_config_from_file
#from kasaya.core import client
from kasaya import async



if __name__=="__main__":
    import sys
    #print ">>>", async.fikumiku.do_work("parameter", 1, foo=123, baz=True )
    for a in range(1000):
        async.locka.burak()
        #async.locka.task_a("!")
        print ".",
        sys.stdout.flush()

#    load_config_from_file("example.conf", optional=True)
#
#    # wywołanie synchroniczne, anonimowe
#    res = sync.fikumiku.do_work("parameter", 1, foo=123, baz=True )
#    print "wynik fikumiku.do_work:" , repr(res)
#
#    # dwa wywołania synchroniczne z prawami usera "roman"
#    with sync("roman") as S:
#        S.fikumiku.do_work("parameter", 2, foo=456, baz=True )
#        S.fikumiku.another_task("important parameter")
#
#    # wywołanie asynchroniczne,
#    # user o nazwie "stefan"
#    print "async"
#    tid = async("stefan").fikumiku.do_work("trololo", 3, foo=567, baz=False)
#    print async_result(tid, "stefan")
#    #print sync("stefan").async_daemon.get_result(tid)
#    tasks = []
#    print "long"
#    for i in range(50):
#        tid = async.fikumiku.long_task(1, i)
#        print "res", tid
#        print sync.async_daemon.get_task_result(tid)
#        tasks.append(tid)
#    import time
#    time.sleep(5)
#    print len(tasks)
#    for t in tasks:
#        print sync.async_daemon.get_task_result(t)

    #sync.fikumiku.wyjebka(234)