#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib.system import all_interfaces
from kasaya.core import exceptions
from kasaya.conf import settings
from kasaya.core.lib import LOG
import socket

__all__=("ConnectionClosed", "NoData", "decode_addr",
         "send_without_response",
         "serialize_and_send",  # TODO: remove this export
         "receive_and_deserialize",
         "send_and_receive", "send_and_receive_response")


# internal exceptions
class ConnectionClosed(exceptions.NetworkError):
    """
    exception throwed in case of transmission,
    when current socket connection is unavailable
    sender should try to repeat transimission after some time
    """
    pass
class NoData(exceptions.NetworkError):
    """
    Connection is closed normally, no more data will be received
    """
    pass



# low level functions
# -------------------


def decode_addr(addr):
    if addr.startswith("tcp://"):
        addr = addr[6:]
        addr,port = addr.split(':',1)
        port = int(port.rstrip("/"))

        if addr.lower()=="local":
            addr = "127.0.0.1"

        elif addr.lower()=="auto":
            addr = "0.0.0.0"

        else:
            for name, ip in all_interfaces().items():
                if name==addr:
                    addr = ip
                    break
        # match ip numbers only
        #re.match( "^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", addr )

        return ( 'tcp', (addr, port), socket.AF_INET, socket.SOCK_STREAM )
    elif addr.startswith("ipc://"):
        return ( 'ipc', addr[6:], socket.AF_UNIX, socket.SOCK_STREAM )


def serialize_and_send(SOCK, serializer, message, timeout=None, resreq=True):
    """
    Send message throught socket.
    Serialization is done by serializer.
    Possible exceptions:
    ConnectionClosed
    """
    if SOCK is None:
        raise ConnectionClosed
    message = serializer.serialize(message, resreq=resreq)
    try:
        SOCK.sendall( message )
    except socket.error as e:
        #if e.errno in (errno.EPIPE, errno.ECONNRESET):
        raise ConnectionClosed("connection closed or pipe broken")
        #raise e
    except Exception as e:
        # shouldn't happen....
        raise ConnectionClosed("abnormal connection error")


def receive_and_deserialize(SOCK, serializer, timeout=None):
    """
    Receive packet from socket and deserialize.
    Result is decoded message.
    Possible exceptions:
    ConnectionClosed, NoData and all exceptions coming from serializer
    """
    try:
        # receiving header
        hsize=serializer.header.size
        header = SOCK.recv(serializer.header.size)
        if not header:
            # there is no more data to receive
            raise NoData

        while len(header)<hsize:
            rest = hsize-len(header)
            data = SOCK.recv(rest)
            if not data:
                # transmission broken before receiving full header
                raise ConnectionClosed
            else:
                header += data
        psize, iv, cmpr, trim, resreq = serializer.decode_header(header)

        # receiving data
        body = SOCK.recv(psize)
        if not body:
            # transmission broken before receiving message body
            raise ConnectionClosed
        while len(body)<psize:
            rest = psize-len(body)
            data = SOCK.recv( rest )
            if not data:
                # transmission broken before receiving full message body
                raise ConnectionClosed("connection closed or pipe broken")
            body += data

    except socket.error as e:
        # socket error
        #print ("ERROR NO",e.errno)
        #if e.errno==errno.ECONNRESET:
            # connection reset by peer (104)
        raise ConnectionClosed("connection closed or pipe broken")
        #else:
        #    raise ConnectionClosed("abnormal connection error")

    except Exception as e:
        # other unknown error
        raise ConnectionClosed("abnormal connection error")

    return serializer.deserialize(
        ((psize, iv, cmpr, trim, resreq),
        body)
    )


# high level
# ----------


def send_without_response(address, message):
    """
    address - full destination address (eg: tcp://127.0.0.1:1234)
    message - message payload (will be automatically serialized)
    """
    print ("send_without_response", address, message)
    serializer = Serializer() # <-- serializer is a singleton
    typ, addr, so1, so2 = decode_addr(address)
    SOCK = socket.socket(so1,so2)
    SOCK.connect(addr)
    # send...
    serialize_and_send(SOCK, serializer, message, resreq=False)
    SOCK.close()


def send_and_receive(address, message, timeout=None):
    """
    address - full destination address (eg: tcp://127.0.0.1:1234)
    message - message payload (will be automatically serialized)
    timeout - time in seconds after which TimeoutError will be raised
    """
    print ("send_and_receive", address, message)
    serializer = Serializer() # <-- serializer is a singleton
    typ, addr, so1, so2 = decode_addr(address)
    SOCK = socket.socket(so1,so2)
    SOCK.connect(addr)
    # send...
    serialize_and_send(SOCK, serializer, message, resreq=True)

    # receive response
    try:
        if timeout is None:
            res, resreq = receive_and_deserialize(SOCK, serializer)
        else:
            # throw exception after timeout and close socket
            try:
                with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                    res, resreq = receive_and_deserialize(SOCK, serializer)
            except exceptions.ReponseTimeout:
                raise exceptions.ReponseTimeout("Response timeout")
    finally:
        SOCK.close()
    return res


def send_and_receive_response(address, message, timeout=None):
    """
    Extended version of send_and_receive. Response is automatically decoded and value is returned.
    If incoming result is exception, then exception will be unpacked and raised.
    If incoming response is not carrying any result, ServiceBusException exception will be thrown.
    """
    result = send_and_receive(address, message, timeout)
    typ = result['message']
    if typ==messages.RESULT:
        return result['result']
    elif typ==messages.NOOP:
        return None

    elif typ==messages.ERROR:
        e = messages.message2exception(result)
        if e is None:
            raise exceptions.MessageCorrupted()
        raise e
    else:
        raise exceptions.ServiceBusException("Wrong message type received")

