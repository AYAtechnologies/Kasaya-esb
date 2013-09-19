#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.core.worker import WorkerDaemon
import sys,os
#from pprint import pprint
#pprint (sys.path)
#pprint (os.environ.items())

if __name__=="__main__":
    servicename = os.environ['SV_SERVICE_NAME']
    module = os.environ['SV_MODULE_IMPORT']

    cwd = os.getcwd()
    if not cwd in sys.path:
        sys.path.append(cwd)
    __import__(module)

    worker = WorkerDaemon(servicename)
    worker.run()

