#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
print esbpath
sys.path.append( esbpath )


from servicebus.syncd import SyncDaemon

if __name__=="__main__":
    daemon = SyncDaemon()
    daemon.run()
