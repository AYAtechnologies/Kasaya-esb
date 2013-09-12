#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

from kasaya import conf
from kasaya.servicebus import client
from kasaya.servicebus.client import sync, async, async_result, control, trans

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
    #print "async"
    #tid = async("stefan").fikumiku.do_work("trololo", 3, foo=567, baz=False)
    #print async_result(tid, "stefan")
    #print sync("stefan").async_daemon.get_result(tid)

    with trans(("dupa","jasiu")) as t:
        t.fikumiku.do_work("parameter", 2, foo=456, baz=True)
        t.fikumiku.another_task("important parameter")

    #sync.fikumiku.waligora("123")
    #sync.fikumiku.wyjebka(234)

    #with async("zyga") as A:
    #    A.fikumiku.wyjebka(234)

    '''
    print sync.async_daemon.get_result(tid)
    print "long"
    tid = async.fikumiku.long_task(1)
    print "res", tid
    print sync.async_daemon.get_result(tid)
    import time
    time.sleep(1)
    print sync.async_daemon.get_result(tid)

    #


    '''
