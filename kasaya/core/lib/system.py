#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import settings
from gevent import socket
import resource


def get_memory_used():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

def get_hostname():
    return socket.gethostname()