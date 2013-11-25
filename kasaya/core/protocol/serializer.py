#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
from kasaya.core import exceptions
from kasaya.core.lib import LOG
import struct
from decimal import Decimal
import datetime


__all__ = ("SingletonSerializer",)


def _load_passwd():
    import hashlib
    try:
        p = bytes(settings.PASSWORD, "ascii")
        return hashlib.sha256(p).digest()
    except TypeError:
        p = settings.PASSWORD
        return hashlib.sha256(p).digest()




class ConfiguredSerializer(object):


    def make_header(self, datasize, trim, iv=b"", response_required=True):
        """
        busname - 6 character long service bus name
        version - protocol version
        datasize - length of data to send
        iv - initial vector used for encryption
        """

        return self.header.pack(
            self._busname, # service bus name
            self._version, # protocol version
            datasize, # size of data in packet
            iv, # initial vector
            False, # compression
            trim,
            response_required,
            )


    def decode_header(self, packet):
        if len(packet)<self.header.size:
            raise exceptions.MessageCorrupted()

        busname, ver, psize, iv, cmpr, trim, resreq = self.header.unpack(packet)

        # not our service bus
        if busname!=self._busname:
            raise exceptions.NotOurMessage()

        # check protocol version
        if ver!=self._version:
            raise exceptions.ServiceBusException("Unknown service bus protocol version")

        return (psize, iv, cmpr, trim, resreq)



    def _plain_serialize(self, msg, resreq=True):
        try:
            payload = self.data_2_bin(msg)
        except Exception as e:
            raise exceptions.SerializationError("Serialization error")
        h = self.make_header(
            len(payload),
            0,
            response_required=resreq )
        return h+payload


    def _plain_deserialize(self, msg):
        # raw message or header, data tuple?
        if type(msg) is tuple:
            psize, iv, cmpr, trim, resreq = msg[0]
            msg = msg[1]
        else:
            HS = self.header.size
            header = msg[:HS]
            msg = msg[HS:]
            psize, iv, cmpr, trim, resreq = self.decode_header(header)

        # check data size declared in header
        #if (len(msg)-HS) != psize:
        #    raise exceptions.MessageCorrupted()

        try:
            return self.bin_2_data(msg), resreq
        except:
            raise exceptions.MessageCorrupted()


    def _encrypted_serialize(self, msg, resreq=True):
        pack = self.data_2_bin(msg)
        try:
            pack = self.data_2_bin(msg)
        except Exception as e:
            raise exceptions.SerializationError("Serialization error")
        # encryption
        pack = self.encrypt(pack, self._passwd, compress=settings.COMPRESSION)
        h = self.make_header(
            len(pack['payload']),
            pack['trim'],
            pack['iv'],
            response_required=resreq )
        return h+pack['payload']


    def _encrypted_deserialize(self, msg):
        """
        msg - message or header tuple of header parameters and message body (tithout header)
        """
        if type(msg) is tuple:
            psize, iv, cmpr, trim, resreq = msg[0]
            msg = msg[1]
        else:
            HS = self.header.size
            header = msg[:HS]
            msg = msg[HS:]
            psize, iv, cmpr, trim, resreq = self.decode_header(header)

        # decrypt
        try:
            pckt = {
                "iv":iv,
                "payload":msg,
                "trim":trim
                }
            msg = self.decrypt(pckt, self._passwd)
        except Exception:
            raise exceptions.MessageCorrupted()

        # unpack message
        return self.bin_2_data(msg), resreq



    def __init__(self, silentinit=False):
        # binary header
        import struct
        #
        # header format
        #  ! big-endian
        #  6s - 6 characters of header prefix
        #  h - protocol version (=1)
        #  L - unsigned long - data size
        #  16s - 16 bytes of initial vector (used for encryption)
        #  ? - boolean - compression flag
        #  H - unsigned short - how many bytes trim from data
        #  ? - boolean - response required
        #
        self.header = struct.Struct(b"!6s h L 16s ? H ?")
        self._version = 1

        # encrypted or not...
        if settings.ENCRYPTION:
            # encrypter, decrypter
            from kasaya.core.protocol.encryption import encrypt, decrypt
            self.encrypt = encrypt
            self.decrypt = decrypt

            self.serialize = self._encrypted_serialize
            self.deserialize = self._encrypted_deserialize
            self._passwd = _load_passwd()
        else:
            self.serialize = self._plain_serialize
            self.deserialize = self._plain_deserialize

        # servicebus name
        import sys
        py3 = sys.version_info>=(3,0)
        if py3:
            busname = bytes(settings.SERVICE_BUS_NAME, "ascii")
            busname += b" "* (6-len(busname))
        else:
            busname = str(settings.SERVICE_BUS_NAME)
            busname += b" "* (6-len(busname))
        self._busname = busname

        # transport protocol
        if settings.TRANSPORT_PROTOCOL=="pickle":
            from kasaya.core.protocol.transport.tr_pickle import bin_2_data, data_2_bin

        elif settings.TRANSPORT_PROTOCOL=="bson":
            if py3:
                # python 3 bson
                from kasaya.core.protocol.transport.tr_bson3 import bin_2_data, data_2_bin
            else:
                # python 2 bson
                from kasaya.core.protocol.transport.tr_bson2 import bin_2_data, data_2_bin

        elif settings.TRANSPORT_PROTOCOL=="msgpack":
            from kasaya.core.protocol.transport.tr_msgpack import bin_2_data, data_2_bin
        else:
            raise Exception("Unsupported transport protocol %s" % settings.TRANSPORT_PROTOCOL)

        self.bin_2_data = bin_2_data
        self.data_2_bin = data_2_bin

        if silentinit:
            return
        LOG.debug("Service bus is configured to use %s as transport protocol." % settings.TRANSPORT_PROTOCOL )




from kasaya.core import SingletonCreator
class SingletonSerializer(ConfiguredSerializer):
    __metaclass__ = SingletonCreator
