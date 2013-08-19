#coding: utf-8
from servicebus.worker import WorkerDaemon
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(esbpath)

from servicebus.asyncd.backend import DictBackend as Backend
from servicebus.conf import load_config_from_file
from servicebus.asyncd import AsyncDeamon

from servicebus.worker import WorkerDaemon
if __name__=="__main__":
    load_config_from_file("../config.txt")
    daemon = AsyncDeamon()
    daemon.run()