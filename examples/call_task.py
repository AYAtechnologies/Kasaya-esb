#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

from kasaya.conf import load_config_from_file
#from kasaya.core import client
from kasaya import sync, async, control, trans

if __name__=="__main__":
    load_config_from_file("example.conf", optional=True)

    # wywołanie synchroniczne, anonimowe
    res = sync.myservice.do_work("parameter", 1, foo=123, baz=True )
    print "wynik fikumiku.do_work:" , repr(res)

    # dwa wywołania synchroniczne z prawami usera "roman"
    with sync("roman") as S:
        S.myservice.do_work("parameter", 2, foo=456, baz=True )
        S.myservice.another_task("important parameter")

    with trans(("dupa","jasiu")) as t:
        t.myservice.do_work("parameter", 2, foo=456, baz=True)
        t.myservice.another_task("important parameter")

    try:
        sync.myservice.wrong(234)
    except Exception as e:
        print "Exception:",e
