#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.conf import settings
import resource
from gevent import socket


def get_memory_used():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

def get_hostname():
    return socket.gethostname()