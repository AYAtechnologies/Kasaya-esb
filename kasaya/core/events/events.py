#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
import gevent

EVENTDB = {}


def add_event_handler(name, func):
    global EVENTDB
    try:
        ev = EVENTDB[name]
    except KeyError:
        ev = []
    if not func in ev:
        ev.append(func)
    EVENTDB[name] = ev


class OnEvent(object):

    def __init__(self, name):
        self.name = name.strip().lower()

    def __call__(self, func):
        add_event_handler(self.name, func)
        return func


def emit(name, *args, **kwargs):
    global EVENTDB
    try:
        funclist = EVENTDB[name]
    except KeyError:
        return

    for f in funclist:
        G = gevent.Greenlet(f, *args, **kwargs)
        G.start()


def _purge_event_db():
    global EVENTDB
    EVENTDB = {}
