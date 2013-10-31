#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib import LOG
from kasaya.core import exceptions
#from kasaya.conf import settings
from binascii import hexlify
import traceback, sys, os
from gevent.server import StreamServer
from gevent import socket
import gevent



def decode_addr(addr):
    if addr.startswith("ipc://"):
        return ( 'ipc', addr[6:], socket.AF_UNIX, socket.SOCK_STREAM )
    elif addr.startswith("tcp://"):
        addr = addr[6:]
        addr,port = addr.split(':',1)
        port = int(port.rstrip("/"))
        return ( 'tcp', (addr, port), socket.AF_INET, socket.SOCK_STREAM )


class MessageLoop(object):

    def __init__(self, address, maxport=None, backlog=50):
        self.is_running = True
        self._msgdb = {}
        # bind to socket
        self.socket_type, addr, so1, so2 = decode_addr(address)
        sock = socket.socket(so1, so2)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if self.socket_type=="ipc":
            os.unlink(addr)
            sock.setblocking(0)
        sock.bind(addr)
        sock.listen(backlog)
        self.SERVER = StreamServer(sock, self.handler)
        # current address
        if self.socket_type=="tcp":
            self.ip, self.port = self.SERVER.address
            self.address = "tcp://%s:%i" % (self.ip, self.port)
        elif self.socket_type=="ipc":
            self.address = "ipc://%s" % addr
        # serialization
        self.serializer = Serializer()

    #def _connect(self, addr, port, maxport=None):
        #self.SOCK, addr# = connector(self.__context)

    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        self.is_running = False

    def close(self):
        #self.SOCK.close()
        pass

    def loop(self):
        self.SERVER.serve_forever()


    def register_message(self, message, func, raw_msg_response=False):
        """
            message - handled message type
            func - handler function
            raw_msg_response - True means that function returns complete message,
                               False - result shoult be packed to message outside handler
        """
        self._msgdb[message]=(func, raw_msg_response)

    def handler(self, SOCK, address):
        print ("connection from", repr(address) )
        # receive data
        msgdata = b""
        while True:
            data = SOCK.recv(4096)
            if not data:
                break
            msgdata += data

        # deserialize
        try:
            msgdata = self.serializer.deserialize(msgdata)

        except exceptions.NotOurMessage:
            # not our servicebus message
            self.send_noop()
            return

        except Exception as e:
            self.send_noop(SOCK)
            LOG.warning("Message deserialisation error")
            LOG.debug("Broken message body dump in hex (only first 1024 bytes):\n%s" % hexlify(msgdata[:1024]))
            return

        try:
            msg = msgdata['message']
        except KeyError:
            self.send_noop(SOCK)
            LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
            return

        # find handler
        try:
            handler, rawmsg = self._msgdb[ msg ]
        except KeyError:
            # unknown messages are ignored
            self.send_noop(SOCK)
            LOG.warning("Unknown message received [%s]" % msg)
            LOG.debug("Message body dump:\n%s" % repr(msgdata) )
            return

        # run handler
        try:
            result = handler(msgdata)
        except Exception as e:
            result = exception_serialize(e, False)
            self.send(SOCK, result)
            LOG.info("Exception [%s] when processing message [%s]. Message: %s." % (result['name'], msg, result['description']) )
            LOG.debug("Message dump: %s" % repr(msgdata) )
            LOG.debug(result['traceback'])
            return

        # send result
        if rawmsg:
            self.send(SOCK, result )
        else:
            self.send(SOCK, {"message":messages.RESULT, "result":result } )


    def send_noop(self, SOCK):
        noop = {"message":messages.NOOP}
        SOCK.send( self.serializer.serialize(noop) )


    def send(self, SOCK, message):
        try:
            packet = self.serializer.serialize(message)
        except exceptions.SerializationError as e:
            try:
                packet = exception_serialize_internal( str(e) )
                packet = self.serializer.serialize(packet)
            except:
                self.send_noop()
                return
        SOCK.sendall( packet )



def send_and_receive(address, message, timeout=10):
    """
    context - ZMQ context
    address - full ZMQ destination address (eg: tcp://127.0.0.1:1234)
    message - message payload (will be automatically serialized)
    timeout - time in seconds after which TimeoutError will be raised
    """
    global serializer
    timeout=3
    try:
        S = serializer
    except NameError:
        serializer = Serializer()
        S = serializer

    #gevent.socket
    typ, addr, so1, so2 = decode_addr(address)
    SOCK = socket.socket(so1,so2)
    SOCK.connect(addr)
    # send message
    print ("send...")
    SOCK.sendall( serializer.serialize(message) )
    # receive response
    if timeout is None:
        res = b""
        while True:
            data = SOCK.recv(4096)
            if not data:
                break
            res += data

        # don't use timeout
        # it's dangerous, can lock client permanently
        # if sender die before sending any response

        #res = SOCK.recv()
    else:
        # throw exception after timeout and close socket
        try:
            with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                res = b""
                while True:
                    data = SOCK.recv(4096)
                    print (".")
                    if not data:
                        break
                    res += data
        except exceptions.ReponseTimeout:
            SOCK.close()
            raise exceptions.ReponseTimeout("Response timeout")
    res = serializer.deserialize(res)
    SOCK.close()
    return res


def send_and_receive_response(address, message, timeout=10):
    """
    j.w. ale dekoduje wynik i go zwraca, lub rzuca otrzymany w wiadomości exception
    """
    result = send_and_receive(context, address, message, timeout)
    typ = result['message']
    if typ==messages.RESULT:
        return result['result']

    elif typ==messages.ERROR:
        e = exception_deserialize(result)
        if e is None:
            raise exceptions.MessageCorrupted()
        raise e
    else:
        raise exceptions.ServiceBusException("Wrong message type received")



def exception_serialize(exc, internal=None):
    """
    Serialize exception object into message
    """
    # try to extract traceback
    tb = traceback.format_exc()

    if sys.version_info<(3,0):
        # python 2
        if not type(tb)==unicode:
            try:
                tb = unicode(tb,"utf-8")
            except:
                tb = repr(tb)

        # error message
        errmsg = exc.message
        try:
            errmsg = unicode(errmsg, "utf-8")
        except:
            errmsg = repr(errmsg)

    else:
        # python 3
        errmsg = str(exc)

    if internal is None:
        # try to guess if exception is servicebus internal internal,
        # or client code external exception
        internal = isinstance(exc, exceptions.ServiceBusException)

    return {
        "message" : messages.ERROR,
        "name" : exc.__class__.__name__,
        "description" : errmsg,
        "internal" : internal,
        "traceback" : tb,
    }



def exception_serialize_internal(description):
    """
    Simple internal errors serializer
    """
    return {
        "message" : messages.ERROR,
        "description" : description,
        "internal" : True,
    }



def exception_deserialize(msg):
    """
    Deserialize exception from message into exception object which can be raised.
    #If message doesn't contains exception, then result will be None.
    """
    if msg['internal']:
        e = exceptions.ServiceBusException(msg['description'])
    else:
        e = Exception(msg['description'])
    try:
        e.name = msg['name']
    except KeyError:
        e.name = "Exception"
    try:
        tb = msg['traceback']
    except KeyError:
        tb = None
    if not tb is None:
        e.traceback = tb
    return e
