#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol.encryption import encrypt, decrypt
from kasaya.conf import settings
from kasaya.core import exceptions
from kasaya.core.lib import LOG
import struct
from decimal import Decimal
import datetime


__all__ = ("serialize", "deserialize")
__headersize = 30
__headerfmt = b"!6s h L 16s H"


def make_header(busname, version, size, trim, iv=b""):
    """
    busname - 6 character long service bus name
    version - protocol version
    payload - data to store in packet
    iv - initial vector used for encryption
    """
    return struct.pack( __headerfmt,
            busname, # service bus name
            version, # protocol version
            size, # size of data in packet
            iv, # initial vector
            trim)

def decode_header(packet):
    return struct.unpack( __headerfmt, packet)



def plain_serialize(msg):
    global __busname
    try:
        payload = data_2_bin(msg)
    except Exception as e:
        raise exceptions.SerializationError("Serialization error")
    h = make_header( __busname, 1, len(payload), 0 )
    return h+payload


def plain_deserialize(msg):
    global __busname
    # minimal sensible packet has at least header
    if len(msg)<__headersize:
        raise exceptions.MessageCorrupted()

    busname, ver, psize, iv, trim = decode_header(msg[:__headersize])

    if ver!=1:
        raise exceptions.ServiceBusException("Unknown service bus protocol version")

    # check data size declared in header
    if (len(msg)-__headersize) != psize:
        raise exceptions.MessageCorrupted()

    # is this message coming from our servicebus?
    try:
        if busname != __busname:
            raise exceptions.NotOurMessage()
    except KeyError:
        raise exceptions.NotOurMessage()

    try:
        return bin_2_data(msg[__headersize:])
    except:
        raise exceptions.MessageCorrupted()




def encrypted_serialize(msg):
    global __passwd, __busname
    try:
        pack = data_2_bin(msg)
    except Exception as e:
        raise exceptions.SerializationError("Serialization error")
    # encryption
    pack = encrypt(pack, __passwd, compress=settings.COMPRESSION)
    h = make_header( __busname, 1,
        len(pack['payload']),
        pack['trim'],
        pack['iv'] )
    return h+pack['payload']



def encrypted_deserialize(msg):
    global __passwd, __busname

    # minimal sensible packet has at least header
    if len(msg)<__headersize:
        raise exceptions.MessageCorrupted()

    busname, ver, psize, iv, trim = decode_header(msg[:__headersize])

    # check protocol version
    if ver!=1:
        raise exceptions.ServiceBusException("Unknown service bus protocol version")

    # is this message coming from our servicebus?
    try:
        if busname != __busname:
            raise exceptions.NotOurMessage()
    except KeyError:
        raise exceptions.NotOurMessage()

    try:
        pckt = {
            "iv":iv,
            "payload":msg[__headersize:],
            "trim":trim
            }
        msg = decrypt(pckt, __passwd)
    except Exception:
        raise exceptions.MessageCorrupted()

    # unpack message
    return bin_2_data(msg)




def prepare_serializer():
    global __passwd, __busname
    global bin_2_data, data_2_bin
    global serialize, deserialize

    import hashlib
    import sys
    py3 = sys.version_info>=(3,0)

    if py3:
        __busname = bytes(settings.SERVICE_BUS_NAME, "ascii")
        __busname += b" "* (6-len(__busname))
    else:
        __busname = str(settings.SERVICE_BUS_NAME)
        __busname += b" "* (6-len(__busname))

    if settings.ENCRYPTION:
        try:
            p = bytes(settings.PASSWORD, "ascii")
            __passwd = hashlib.sha256(p).digest()
        except TypeError:
            p = settings.PASSWORD
            __passwd = hashlib.sha256(p).digest()
        serialize = encrypted_serialize
        deserialize = encrypted_deserialize
        from binascii import hexlify

    else:
        serialize = plain_serialize
        deserialize = plain_deserialize

    # transport protocol
    if settings.TRANSPORT_PROTOCOL=="pickle":
        # pickle !
        from kasaya.core.protocol.transport.tr_pickle import bin_2_data, data_2_bin
        #LOG.warning("Service bus is configured to use pickle as transport protocol.")

    elif settings.TRANSPORT_PROTOCOL=="bson":
        if py3:
            # python 3 bson
            from kasaya.core.protocol.transport.tr_bson3 import bin_2_data, data_2_bin
        else:
            # python 2 bson
            from kasaya.core.protocol.transport.tr_bson2 import bin_2_data, data_2_bin

    elif settings.TRANSPORT_PROTOCOL=="msgpack":
        from transport.tr_msgpack import bin_2_data, data_2_bin

    else:
        raise Exception("Unsupported transport protocol %s" % settings.TRANSPORT_PROTOCOL)


prepare_serializer()
