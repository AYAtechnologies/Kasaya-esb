#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
__author__ = 'wektor'

from unittest import TestCase

import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )

from servicebus import client, conf
from servicebus.client import sync, async, register_auth_processor, async_result
import datetime
import time
import subprocess


class TestAsync(TestCase):

    @classmethod
    def setUpClass(cls):
        conf.load_config_from_file("../config.txt")
        PYTHON = "~/PycharmProjects/django-env/bin/python" #PYTHON_INTERPRETER
        # subprocess.call(PYTHON + " ../examples/syncserver/run_syncd.py", shell=True)
        # subprocess.call(PYTHON + " ../examples/syncserver/run_async_worker.py", shell=True)
        # subprocess.call(PYTHON + " ../examples/workers/simple_worker.py", shell=True)


    def test_register(self):
        tid = async.fikumiku.long_task(1, 1)
        print "res", tid

    def test_in_progress(self):
        print "start"
        tid = async.fikumiku.long_task(1, 1)
        print "res", tid
        res = async_result(tid, "stefan")
        assert res == ['in_progress', None]
        time.sleep(2)
        res = async_result(tid, "stefan")
        print res
        assert res == ['ok', "hurra 1"]

    def test_worker_error(self):
        pass

    def test_long_wait_with_large_group(self):
        tasks = []
        print "long"
        TASK_COUNT = 10 # Docelowo ta liczba powinna byc duzo wieksza - narazie jest mala
        for i in range(TASK_COUNT):
            tid = async.fikumiku.long_task(1, i)
            print "res", tid
            print sync.async_daemon.get_task_result(tid)
            tasks.append(tid)
        import time
        time.sleep(2)
        assert len(tasks) == TASK_COUNT
        for t in tasks:
            res = sync.async_daemon.get_task_result(t)
            assert res[0] == "ok" or res[0] == "in_progress"

    def test_large_group(self):
        tasks = []
        t0 = datetime.datetime.now()
        print t0
        print "long"
        TASK_COUNT = 50
        for i in range(TASK_COUNT):
            tid = async.fikumiku.long_task(0, i)
            print "res", tid
            print sync.async_daemon.get_task_result(tid)
            tasks.append(tid)
        time.sleep(2)
        assert len(tasks) == TASK_COUNT
        for t in tasks:
            res = sync.async_daemon.get_task_result(t)
            assert res[0] == "ok"
        print datetime.datetime.now() - t0


    def test_performance(self):
        pass

    def test_redis_backend(self):
        pass

    def test_concurrency(self):
        pass

    def test_cleanup_after_demon_fail(self):
        # jeden przyjmuje taski i pada
        # drugi po starcie sprawdza baze - widzie ze taski leza i je przejmuje
        pass

    def test_global_timeout(self): # global = ustawiony gdzies w bazie
        pass

    def test_local_timeout(self): # local = ustawiony przy wywolaniu
        pass
