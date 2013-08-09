#coding: utf-8
from servicebus.conf import settings
import msgpack
import datetime
import encryption

__all__ = ("serialize", "deserialize")

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



def serialize(msg):
    msg = msgpack.packb(msg, default=encode_ext_types)
    if settings.ENCRYPTION:
        meta = encryption.encrypt(msg, settings.PASSWORD, compress=settings.COMPRESSION)
        return msgpack.packb(meta)
    return msg


def deserialize(msg):
    if settings.ENCRYPTION:
        msg = msgpack.unpackb(msg)
        msg = encryption.decrypt(msg, settings.PASSWORD)
    data = msgpack.unpackb(msg, object_hook=decode_obj_types)
    return data

