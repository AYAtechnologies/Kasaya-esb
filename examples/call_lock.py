#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
#from kasaya.conf import load_config_from_file
from kasaya import sync


if __name__=="__main__":
    #load_config_from_file("example.conf", optional=True)
    res = sync.locka.task_a("start!")
    print res