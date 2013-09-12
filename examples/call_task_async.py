#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

from servicebus import client, conf
from servicebus.client import sync, async, register_auth_processor, async_result, control

if __name__=="__main__":
    conf.load_config_from_file("kasaya.conf")

    # wywołanie synchroniczne, anonimowe
    res = sync.fikumiku.do_work("parameter", 1, foo=123, baz=True )
    print "wynik fikumiku.do_work:" , repr(res)

    # dwa wywołania synchroniczne z prawami usera "roman"
    with sync("roman") as S:
        S.fikumiku.do_work("parameter", 2, foo=456, baz=True )
        S.fikumiku.another_task("important parameter")

    # wywołanie asynchroniczne,
    # user o nazwie "stefan"
    print "async"
    tid = async("stefan").fikumiku.do_work("trololo", 3, foo=567, baz=False)
    print async_result(tid, "stefan")
    #print sync("stefan").async_daemon.get_result(tid)
    tasks = []
    print "long"
    for i in range(50):
        tid = async.fikumiku.long_task(1, i)
        print "res", tid
        print sync.async_daemon.get_task_result(tid)
        tasks.append(tid)
    import time
    time.sleep(5)
    print len(tasks)
    for t in tasks:
        print sync.async_daemon.get_task_result(t)

    #sync.fikumiku.wyjebka(234)