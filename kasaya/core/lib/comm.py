#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib import LOG
from kasaya.conf import settings
from kasaya.core import exceptions
from binascii import hexlify
import traceback, sys, os
from gevent.server import StreamServer
from gevent import socket
import gevent, errno


# internal exceptions


class ConnectionClosed(Exception):
    """
    Connection is closed before receiving full message
    """
    pass
class NoData(Exception):
    """
    Connection is closed normally, no more data will be received
    """
    pass
class ConnectionClosed(Exception):
    """
    exception throwed in case of transmission,
    when current socket connection is unavailable
    sender should try to repeat transimission after some time
    """
    pass


# low level functions


def decode_addr(addr):
    if addr.startswith("tcp://"):
        addr = addr[6:]
        addr,port = addr.split(':',1)
        port = int(port.rstrip("/"))
        return ( 'tcp', (addr, port), socket.AF_INET, socket.SOCK_STREAM )
    elif addr.startswith("ipc://"):
        return ( 'ipc', addr[6:], socket.AF_UNIX, socket.SOCK_STREAM )



def _serialize_and_send(SOCK, serializer, message, timeout=None):
    """
    Send message throught socket.
    Serialization is done by serializer.
    Possible exceptions:
    ConnectionClosed
    """
    message = serializer.serialize(message)
    try:
        SOCK.sendall( message )
    except socket.error as e:
        if e.errno==errno.EPIPE:
            raise ConnectionClosed
        print ("     UNKNOWN EXCEPTION",e)



def _receive_and_deserialize(SOCK, serializer, timeout=None):
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
        psize, iv, cmpr, trim = serializer.decode_header(header)

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
                raise ConnectionClosed
            body += data

    except socket.error as e:
        # socket error
        #print ("ERROR NO",e.errno)
        if e.errno==errno.ECONNRESET:
            # connection reset by peer (104)
            raise ConnectionClosed
        else:
            raise

    return serializer.deserialize(
        ((psize, iv, cmpr, trim),
        body)
    )



# high level functions and classes


class Sender(object):
    """
    Sending multiple messages to one target. Not receiving any data
    autoreconnect - if True enables autoreconnection process
    on_connection_start - when connection is back again, this funcion will be called
    on_connection_close - when connection is losed, this function will be called

    """
    def __init__(self, address, autoreconnect=False, on_connection_start=None, on_connection_close=None ):
        """
        address - destination address inf format: tcp://ipnumber:port
        autoreconnect - will try to connect in background until success if connection fails or is unavailable
        """
        self.__working = False
        self.__recon = None

        # start stop notification functions
        if on_connection_close:
            self.__on_connection_close = on_connection_close
        else:
            self.__on_connection_close = self.__dummy
        if on_connection_start:
            self.__on_connection_start = on_connection_start
        else:
            self.__on_connection_start = self.__dummy

        self.autoreconnect = autoreconnect
        self._address = address
        self.serializer = Serializer()

        # connect or start background connecting process
        if self.autoreconnect:
            self.__start_auto_reconnect()
        else:
            self._connect()

    def __dummy(self): pass


    def _connect(self):
        """
        Connect to address. Return True if success, False if fails.
        """
        self.socket_type, addr, so1, so2 = decode_addr(self._address)
        self.SOCK = socket.socket(so1, so2)
        try:
            self.SOCK.connect(addr)
            self.__working = True
            self.__on_connection_start()
            return True
        except socket.error as e:
            # if autoreconnect is enabled, start process
            if self.autoreconnect:
                #LOG.debug("Connection failed, will try to reconnect in %i seconds" % settings.HEARTBEAT_TIMEOUT)
                #if e.errno==errno.ECONNREFUSED:
                self.__start_auto_reconnect()
                return False
            else:
                # no autoreconnection
                self.__on_connection_close()
                raise

    def __broken_connection(self):
        """
        Connection broken
        """
        if self.__working:
            self.__working = False
            self.__on_connection_close()


    def __start_auto_reconnect(self):
        """
        Starts auto reconnection process in background
        """
        if self.__recon is not None:
            # reconnection is in progress
            return
        if not self.autoreconnect:
            # reconnection is not enabled
            return
        self.__recon = gevent.Greenlet(self.connection_loop)
        self.__recon.start()


    def connection_loop(self):
        deltime = 0.1
        while not self.__working:
            gevent.sleep(deltime)
            succ = self._connect()
            deltime = settings.HEARTBEAT_TIMEOUT
        #LOG.debug("Connection success")
        self.__recon = None


    def close(self):
        """
        Close socket and stop reconnecting process
        """
        if self.__recon is not None:
            self.__recon.kill()
        # not closing if not opened
        if self.__working:
            self.SOCK.close()

    @property
    def is_active(self):
        return self.__working

    def on_connection_close(self, func):
        pass

    def on_connection_start(self, func):
        pass


    # communication


    def send(self, message, timeout=None):
        """
        Sends message to target. Return True if success, False if no connection.
        timeout is not supported yet.
        """
        if not self.__working:
            raise ConnectionClosed
        try:
            _serialize_and_send(self.SOCK, self.serializer, message)
        except ConnectionClosed:
            # notify about connection loose
            self.__broken_connection()
            # run reconnection process
            self.__start_auto_reconnect()
            raise
        #return True


    def send_and_receive(self, message, timeout=10):
        """
        message - message payload (will be automatically serialized)
        timeout - time in seconds after which TimeoutError will be raised.
                  Note that timeout only for receive response, not for sending.
        """
        # send message
        if not self.__working:
            raise ConnectionClosed
        try:
            _serialize_and_send(self.SOCK, self.serializer, message)
        except ConnectionClosed:
            # notify about connection loose
            self.__broken_connection()
            # run reconnection process
            self.__start_auto_reconnect()
            raise

        # receive response
        if timeout is None:
            res = _receive_and_deserialize(self.SOCK, self.serializer, timeout)
        else:
            # throw exception after timeout and close socket
            try:
                with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                    res = _receive_and_deserialize(self.SOCK, self.serializer)
            except exceptions.ReponseTimeout:
                raise exceptions.ReponseTimeout("Response timeout")
        #print ("res",res)
        return res



class MessageLoop(object):
    """
    Loop receiving messages
    """

    def __init__(self, address, maxport=None, backlog=50):
        self.is_running = True
        self._msgdb = {}
        # bind to socket
        self.socket_type, addr, so1, so2 = decode_addr(address)
        sock = socket.socket(so1, so2)
        #sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if self.socket_type=="ipc":
            os.unlink(addr)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(1)
        sock.bind(addr)
        sock.listen(backlog)
        self.SERVER = StreamServer(sock, self.connection_handler)
        # current address
        if self.socket_type=="tcp":
            self.ip, self.port = self.SERVER.address
            self.address = "tcp://%s:%i" % (self.ip, self.port)
        elif self.socket_type=="ipc":
            self.address = "ipc://%s" % addr
        # serialization
        self.serializer = Serializer()


    def kill(self):
        self.SERVER.kill()


    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        self.is_running = False

    def close(self):
        """
        Close socket
        """
        pass
        #self.SOCK.close()

    def loop(self):
        """
        Start infinite loop
        """
        self.SERVER.serve_forever()

    def register_message(self, message, func, raw_msg_response=False):
        """
            message - handled message type
            func - handler function
            raw_msg_response - True means that function returns complete message,
                               False - result shoult be packed to message outside handler
        """
        self._msgdb[message]=(func, raw_msg_response)


    def connection_handler(self, SOCK, address):
        #print ("connection from", repr(address) )
        while True:
            try:
                msgdata = _receive_and_deserialize(SOCK, self.serializer)
            except (NoData, ConnectionClosed):
                return

            try:
                msg = msgdata['message']
            except KeyError:
                self._send_noop(SOCK)
                LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
                continue
            # find handler
            try:
                handler, rawmsg = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored
                self._send_noop(SOCK)
                LOG.warning("Unknown message received [%s]" % msg)
                LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue
            # run handler
            try:
                result = handler(msgdata)
            except Exception as e:
                #result = exception_serialize(e, False)
                _serialize_and_send(
                    SOCK,
                    self.serializer,
                    exception_serialize(e, False)
                )
                LOG.info("Exception [%s] when processing message [%s]. Message: %s." % (result['name'], msg, result['description']) )
                LOG.debug("Message dump: %s" % repr(msgdata) )
                LOG.debug(result['traceback'])
                continue

            # send result
            if rawmsg:
                _serialize_and_send(
                    SOCK,
                    self.serializer,
                    result
                )
            else:
                _serialize_and_send(
                    SOCK,
                    self.serializer, {
                        "message":messages.RESULT,
                        "result":result
                    }
                )

    def _send_noop(self, SOCK):
        """
        Sends empty message
        """
        _serialize_and_send(
            SOCK,
            self.serializer, {
                "message":messages.NOOP,
                "result":result
            }
        )


def send_and_receive(address, message, timeout=10):
    """
    context - ZMQ context
    address - full ZMQ destination address (eg: tcp://127.0.0.1:1234)
    message - message payload (will be automatically serialized)
    timeout - time in seconds after which TimeoutError will be raised
    """
    S = Serializer() # <-- serializer is a singleton

    typ, addr, so1, so2 = decode_addr(address)
    SOCK = socket.socket(so1,so2)
    SOCK.connect(addr)

    # send...
    _serialize_and_send(SOCK, S, message)

    # receive response
    try:
        if timeout is None:
            res = _receive_and_deserialize(SOCK, serializer)
        else:
            # throw exception after timeout and close socket
            try:
                with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                    res = _receive_and_deserialize(SOCK, serializer)
            except exceptions.ReponseTimeout:
                raise exceptions.ReponseTimeout("Response timeout")
    finally:
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


# serialize and deserialize exceptions


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
