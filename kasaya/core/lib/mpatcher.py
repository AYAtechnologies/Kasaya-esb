#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
import os
from kasaya.conf import settings

def damonkey():
    if not os.environ.get('DONT_USE_MONKEY_PATCH',False):
        from gevent import monkey
        monkey.patch_all()
