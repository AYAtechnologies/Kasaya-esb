#coding: utf-8
#
# django integration helpers
#
from __future__ import division, absolute_import, print_function, unicode_literals

try:
    from django.db import close_connection as _close_dj_connection
except Exception:
    _close_dj_connection = lambda:None


def close_django_conn():
    """
    Closing database connection after finishing task
    """
    global _close_dj_connection
    try:
        _close_dj_connection()
    except Exception as e:
        if e.__class__.__name__ == "ImproperlyConfigured":
            # django connection is not required or diango orm is not used at all,
            # because of that we replace _close_dj_connection function by empty lambda
            _close_dj_connection = lambda:None

