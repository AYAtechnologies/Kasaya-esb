#coding: utf-8
from servicebus.conf import settings
from servicebus import exceptions
import msgpack
import datetime
import encryption
from decimal import Decimal


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



def serialize(msg):
    if settings.ENCRYPTION:
        msg = msgpack.packb(msg, default=encode_ext_types)
        meta = encryption.encrypt(msg, settings.PASSWORD, compress=settings.COMPRESSION)
        meta['ver'] = 1
        meta['_n_'] = settings.SERVICE_BUS_NAME
        return msgpack.packb(meta)
    else:
        msg['ver'] = 1
        meta['_n_'] = settings.SERVICE_BUS_NAME
        msg = msgpack.packb(msg, default=encode_ext_types)
        return msg



def deserialize(msg):
    if settings.ENCRYPTION:
        # message is encrypted
        try:
            msg = msgpack.unpackb(msg)
        except msgpack.exceptions.UnpackException:
            raise exceptions.MessageCorrupted()

        # check protocol version
        try:
            if msg['ver']!=1:
                raise exceptions.ServiceBusException("Unknown service bus protocol version")
        except:
            raise exceptions.MessageCorrupted()

        # is this message coming from our servicebus?
        try:
            if not msg['_n_'] == settings.SERVICE_BUS_NAME:
                raise exceptions.NotOurMessage()
        except KeyError:
            raise exceptions.NotOurMessage()

        try:
            msg = encryption.decrypt(msg, settings.PASSWORD)
        except Exception:
            raise exceptions.MessageCorrupted()

        # unpack message
        data = msgpack.unpackb(msg, object_hook=decode_obj_types)
        return data

    else:
        # message is unencrypted
        try:
            data = msgpack.unpackb(msg, object_hook=decode_obj_types)
        except msgpack.exceptions.UnpackException:
            raise exceptions.MessageCorrupted()

        # check protocol version
        try:
            if data['ver']!=1:
                raise exceptions.ServiceBusException("Unknown service bus protocol version")
        except:
            raise exceptions.MessageCorrupted()

        # is this message coming from our servicebus?
        try:
            if not data['_n_'] == settings.SERVICE_BUS_NAME:
                raise exceptions.NotOurMessage()
        except KeyError:
            raise exceptions.NotOurMessage()

        return data

