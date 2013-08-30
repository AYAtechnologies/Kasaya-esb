#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
__author__ = 'wektor'

from unittest import TestCase

import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )

from servicebus import client, conf
from servicebus.client import sync, async, register_auth_processor, async_result, busctl
import datetime
import time
import subprocess


class TestAsync(TestCase):

    @classmethod
    def setUpClass(cls):
        conf.load_config_from_file("../config.txt")
        print conf.settings.MIDDLEWARE
        PYTHON = "~/PycharmProjects/django-env/bin/python" #PYTHON_INTERPRETER
        # subprocess.call(PYTHON + " ../examples/syncserver/run_syncd.py", shell=True)
        # subprocess.call(PYTHON + " ../examples/syncserver/run_async_worker.py", shell=True)
        # subprocess.call(PYTHON + " ../examples/workers/simple_worker.py", shell=True)


    def test_async_success(self):
        tid = async.fikumiku.long_task(1, 1)
        print "res", tid

    def test_async_fail(self):
        tid = async.fikumiku.long_task(1, 1)
        print "res",  tid
        assert tid == None

    def test_sync_success(self):
        tid = sync({"auth":("admin","pass")}).fikumiku.long_task(1, 1)
        print "res", tid
        assert tid == "hurra 1"

    def test_sync_fail(self):
        tid = sync.fikumiku.long_task(1, 1)
        print "res", tid
        assert tid == None
