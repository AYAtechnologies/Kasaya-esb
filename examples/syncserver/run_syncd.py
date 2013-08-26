#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )


from servicebus.conf import load_config_from_file
from servicebus.syncd import SyncDaemon


if __name__=="__main__":
    load_config_from_file("../../config.txt")
    daemon = SyncDaemon()
    daemon.run()
