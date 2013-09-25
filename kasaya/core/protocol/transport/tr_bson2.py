#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core import exceptions
import bson
#
# BSON has two implementations under python 2 and 3
# this is wrapper for python 2 version
#


def data_2_bin(data):
    return bson.dumps(data)

def bin_2_data(bin):
    try:
        return bson.loads(bin)
    except:
        raise exceptions.MessageCorrupted()
