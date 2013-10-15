#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .logger import LOG


def make_kasaya_id(host=False):
    """
    Make 25 character long unique id used in kasaya to identify all hosts.
    if host is True, generated ID is used to identify hosts, not workers.
    """
    import os, hashlib, base64
    h = hashlib.new("md5")
    h.update(os.urandom(256) )
    h = base64.b32encode(h.digest())
    h = h.decode("ascii").rstrip("=")
    if host:
        return "H"+h
    return "W"+h
