#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.lib import LOG, make_kasaya_id


class WorkerBase(object):

    def __init__(self, is_host=False):
        self.ID = make_kasaya_id(host=is_host)

