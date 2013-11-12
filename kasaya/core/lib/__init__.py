#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .logger import LOG


def make_kasaya_id(host=False):
    """
    Make 14 character long unique id used in kasaya to identify all hosts.
    if host is True, generated ID is used to identify hosts, not workers.
    """
    import os, base64
    code = base64.b32encode( os.urandom(8) )
    h = code.decode("ascii").rstrip("=")
    if host:
        return "H"+h
    return "W"+h
