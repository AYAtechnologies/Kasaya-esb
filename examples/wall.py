#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import sync, async, control, trans


if __name__=="__main__":
    #load_config_from_file("example.conf", optional=True)

    # wywołanie synchroniczne, anonimowe
    w = sync.wallet.create("PLN")
    print w
    res = sync.wallet.credit(w, 123, "PLN")
