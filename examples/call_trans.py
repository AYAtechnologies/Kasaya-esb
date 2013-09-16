#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus import client, conf
from servicebus.client import sync, async, control, trans
from servicebus.client import ExecContext

if __name__=="__main__":
    load_config_from_file("example.conf", optional=True)
    with sync(("ala","ma", "kota")) as s:
        s.fikumiku.do_work("sync", 2, foo=456, baz=True)


    e = ExecContext(("kot", "ma", "aids"))
    e.sync.fikumiku.do_work("HOHOHO", 1)

    sync.fikumiku.do_work("HIHIHI", 1999)

    with trans(("dupa","jasiu")) as t:
        t.fikumiku.do_work("parameter", 2, foo=999, baz=False)
        t.fikumiku.another_task("important parameter")

