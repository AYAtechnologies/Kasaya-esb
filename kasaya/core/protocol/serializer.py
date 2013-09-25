#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol.encryption import encrypt, decrypt
from kasaya.conf import settings
from kasaya.core import exceptions
from decimal import Decimal
#import msgpack
import datetime


__all__ = ("serialize", "deserialize")



def plain_serialize(msg):
    message = {
        'ver':1,
        'sb':settings.SERVICE_BUS_NAME,
        'payload':msg,
    }
    return data_2_bin(message)


def plain_deserialize(msg):
    data = bin_2_data(msg)
    # check protocol version
    try:
        if data['ver']!=1:
            raise exceptions.ServiceBusException("Unknown service bus protocol version")
    except:
        raise exceptions.MessageCorrupted()

    # is this message coming from our servicebus?
    try:
        if not data['sb'] == settings.SERVICE_BUS_NAME:
            raise exceptions.NotOurMessage()
    except KeyError:
        raise exceptions.NotOurMessage()

    try:
        return data['payload']
    except:
        raise exceptions.MessageCorrupted()




def encrypted_serialize(payload):
    global __passwd
    payload = data_2_bin(payload)
    message = encrypt(payload, __passwd, compress=settings.COMPRESSION)
    message['ver'] = 1
    message['sb'] = settings.SERVICE_BUS_NAME
    return data_2_bin(message)


def encrypted_deserialize(msg):
    global __passwd
    msg = bin_2_data(msg)
    # check protocol version
    try:
        if msg['ver']!=1:
            raise exceptions.ServiceBusException("Unknown service bus protocol version")
    except:
        raise exceptions.MessageCorrupted()

    # is this message coming from our servicebus?
    try:
        if msg['sb'] != settings.SERVICE_BUS_NAME:
            raise exceptions.NotOurMessage()
    except KeyError:
        raise exceptions.NotOurMessage()

    r = decrypt(msg, __passwd)
    print (r)

    try:
        msg = decrypt(msg, __passwd)
    except Exception:
        raise exceptions.MessageCorrupted()

    # unpack message
    return bin_2_data(msg)




def prepare_serializer():
    global __passwd
    global bin_2_data, data_2_bin
    global serialize, deserialize

    import hashlib
    import sys

    if settings.ENCRYPTION:
        print ("Encryption ON")
        # python 3 hack on password
        try:
            p = bytes(settings.PASSWORD, "ascii")
            __passwd = hashlib.sha256(p).digest()
        except TypeError:
            p = settings.PASSWORD
            __passwd = hashlib.sha256(p).digest()
        serialize = encrypted_serialize
        deserialize = encrypted_deserialize
        from binascii import hexlify
        print ("         ",hexlify(__passwd) )

    else:
        print ("No encryption")
        serialize = plain_serialize
        deserialize = plain_deserialize

    # transport protocol
    if settings.TRANSPORT_PROTOCOL=="bson":
        if sys.version_info<(3,0):
            print ("PYTHON 2")
            # python 2 bson
            from kasaya.core.protocol.transport.tr_bson2 import bin_2_data, data_2_bin
        else:
            print ("PYTHON 3")
            # python 3 bson
            from kasaya.core.protocol.transport.tr_bson3 import bin_2_data, data_2_bin

    elif settings.TRANSPORT_PROTOCOL=="msgpack":
        from transport.tr_msgpack import bin_2_data, data_2_bin

    else:
        raise Exception("Unsupported transport protocol %s" % settings.TRANSPORT_PROTOCOL)


prepare_serializer()
