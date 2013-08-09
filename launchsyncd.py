#!/usr/bin/env python
#coding: utf-8
from syncd import SyncDaemon

if __name__=="__main__":
    daemon = SyncDaemon()
    daemon.run()
