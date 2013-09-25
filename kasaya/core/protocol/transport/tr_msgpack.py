#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core import exceptions
import msgpack
#
#  Warning, msgpack is broken and can't differentiate strings from binary data.
#  Under python 3 message pack is unusable to transport data.
#
#  More details and useless discussion here:
#  https://github.com/msgpack/msgpack/issues/121
#

def encode_ext_types(obj):
    """
    Convert unknown for messagepack protocol types to dicts
    """
    encoders = {
        # datetime
        datetime.datetime: (
            'datetime',
            lambda obj:obj.strftime("%Y%m%dT%H:%M:%S.%f")
        ),

        # date
        datetime.date: (
            'date',
            lambda obj:obj.strftime("%Y%m%d")
        ),

        # time
        datetime.time: (
            'time',
            lambda obj:obj.strftime("%H:%M:%S.%f")
        ),

        # timedelta
        datetime.timedelta: (
            'timedelta',
            lambda obj: "%i:%i:%i" % (obj.days, obj.seconds, obj.microseconds)
        ),

        Decimal: (
            'decimal',
            lambda obj: str(obj)
        )
    }

    key = type(obj)#.__class__
    if key in encoders:
        n,f = encoders[obj.__class__]
        return {'__customtype__':n, 'as_str':f(obj) }
    raise Exception("Encoding of %s is not possible " % key)
    return obj



def decode_obj_types(obj):
    """
    Reverse operation for encode_ext_types
    """
    decoders = {
        'datetime':
            lambda S : datetime.datetime.strptime( S, "%Y%m%dT%H:%M:%S.%f"),
        'date':
            lambda S : datetime.datetime.strptime( S, "%Y%m%d").date(),
        'time':
            lambda S : datetime.datetime.strptime( S, "%H:%M:%S.%f").time(),
        'timedelta':
            lambda S : datetime.timedelta(  **dict( [ (n,int(v)) for n, v in zip(("days","seconds","microseconds"), S.split(":")) ])  ),

        'decimal':
            lambda S : Decimal(S),
    }
    try:
        key = obj['__customtype__']
    except:
        return obj
    try:
        func = decoders[key]
    except KeyError:
        return obj
    return func(obj['as_str'])




def data_2_bin(data):
    return msgpack.packb(data, default=encode_ext_types)

def bin_2_data(bin):
    return msgpack.unpackb(bin, object_hook=decode_obj_types)
    try:
        pass
    except msgpack.exceptions.UnpackException:
        raise exceptions.MessageCorrupted()
