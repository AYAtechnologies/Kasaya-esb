#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core import exceptions

try:
    import cPickle as pickle
except:
    import pickle

#
# Pickle transport is only for testing and development.
# Don't pickling in production environment.
#
# Pickle don't allow to exchange data between python 2 and 3,
# because of python 3 tries to unpack binary data as unicode string.
#


def data_2_bin(data):
    return pickle.dumps(data,2)

def bin_2_data(bin):
    try:
        return pickle.loads(bin)
    except Exception as e:
        raise exceptions.MessageCorrupted()
