#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )


from servicebus import client, conf
from servicebus.client import sync, async, async_result, control, trans
from servicebus.client import ExecContext

if __name__=="__main__":
    conf.load_config_from_file("../../config.txt")
    with sync(("ala","ma", "kota")) as s:
        s.fikumiku.do_work("sync", 2, foo=456, baz=True)


    e = ExecContext(("kot", "ma", "aids"))
    e.sync.fikumiku.do_work("HOHOHO", 1)

    sync.fikumiku.do_work("HIHIHI", 1999)

    with trans(("dupa","jasiu")) as t:
        t.fikumiku.do_work("parameter", 2, foo=999, baz=False)
        t.fikumiku.another_task("important parameter")

