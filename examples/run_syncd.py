#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import load_config_from_file
from kasaya.workers.syncd import SyncDaemon


if __name__=="__main__":
    load_config_from_file("example.conf", optional=True)
    daemon = SyncDaemon()
    daemon.run()
