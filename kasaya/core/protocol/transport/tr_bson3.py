#coding: utf-8
from kasaya.core import exceptions
from . import py3bson
#
# BSON has two implementations under python 2 and 3
# this is wrapper for python 3 version
#


def data_2_bin(data):
    return py3bson.serialize_to_bytes(data)

def bin_2_data(bin):
    try:
        return dict(py3bson.parse_bytes(bin))
    except py3bson.BSON_Error:
        raise exceptions.MessageCorrupted()
