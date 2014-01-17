#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib import LOG
from kasaya.core.events import emit
from kasaya.conf import settings
from kasaya.core import exceptions
from kasaya.core.lib.system import all_interfaces
from binascii import hexlify
import traceback, sys, os
from gevent.server import StreamServer
from gevent import socket
import gevent, errno


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



def _serialize_and_send(SOCK, serializer, message, timeout=None, resreq=True):
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



# high level functions and classes


class Sender(object):
    """
    Sending multiple messages to one target. Not receiving any data
    autoreconnect - if True enables autoreconnection process
    sessionid - optional session id which will be send immediately after connection.
    """
    def __init__(self, address, autoreconnect=False, sessionid=None):
        """
        address - destination address inf format: tcp://ipnumber:port
        autoreconnect - will try to connect in background until success if connection fails or is unavailable
        """
        self.__working = False
        self.__recon = None
        self._address = address
        self.autoreconnect = autoreconnect
        self.__sessionid = sessionid
        self.serializer = Serializer()
        # connect or start background connecting process
        if self.autoreconnect:
            self.__start_auto_reconnect()
        else:
            self._connect()


    def _connect(self):
        """
        Connect to address. Return True if success, False if fails.
        """
        self.SOCK = None
        self.socket_type, addr, so1, so2 = decode_addr(self._address)
        SOCK = socket.socket(so1, so2)
        try:
            SOCK.connect(addr)
            self.__working = True
            emit("sender-conn-started", self._address)
            self.SOCK = SOCK

            # send session id if is configured
            if self.__sessionid!=None:
                msg = {
                    "message" : messages.SET_SESSION_ID,
                    "id" : self.__sessionid,
                }
                try:
                    self.send(msg)
                except ConnectionClosed:
                    return False
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
                emit("sender-conn-closed", self._address)
                raise

    def __broken_connection(self):
        """
        Connection broken
        """
        if self.__working:
            self.__working = False
            self.SOCK = None
            emit("sender-conn-closed", self._address)
            # run reconnection process
            self.__start_auto_reconnect()


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
        #LOG.debug("trying reconnect...")
        emit("sender-conn-reconn", self._address)
        self.__recon = gevent.Greenlet(self.connection_loop)
        self.__recon.start()


    def connection_loop(self):
        deltime = 0.1
        while not self.__working:
            gevent.sleep(deltime)
            succ = self._connect()
            deltime = settings.WORKER_HEARTBEAT / 2.0
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
            _serialize_and_send(self.SOCK, self.serializer, message, resreq=False)
        except ConnectionClosed:
            self.__broken_connection()  # notify about connection loose
            raise


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
            _serialize_and_send(self.SOCK, self.serializer, message, resreq=True)
        except ConnectionClosed:
            self.__broken_connection()  # notify about connection loose
            raise

        # receive response
        if timeout is None:
            res, resreq = _receive_and_deserialize(self.SOCK, self.serializer, timeout)
        else:
            # throw exception after timeout and close socket
            try:
                with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                    res, resreq = _receive_and_deserialize(self.SOCK, self.serializer)
            except exceptions.ReponseTimeout:
                raise exceptions.ReponseTimeout("Response timeout")

        return res



class MessageLoop(object):
    """
    Loop receiving messages
    """

    def __init__(self, address, maxport=None, backlog=50 ):
        # session id is received from connecting side for each connection
        # by default it's unset and unused. When set it will be sended after
        # connection lost in event.
        self._msgdb = {}
        self.__listening_on = []

        # bind to socket
        self.socket_type, addr, so1, so2 = decode_addr(address)
        sock = socket.socket(so1, so2)
        if self.socket_type=="ipc":
            os.unlink(addr)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(1)

        if self.socket_type=="ipc" or (maxport is None):
            # single socket binding
            sock.bind(addr)
        else:
            # binding to one from range of available ports
            while True:
                try:
                    sock.bind(addr)
                    break # success
                except socket.error as e:
                    if e.errno==errno.EADDRINUSE:
                        ip,port = addr
                        if port<maxport:
                            addr = (ip,port+1)
                            continue
                        else:
                            # whole port range is used
                            raise
                    else:
                        # other socket errors...
                        raise

        sock.listen(backlog)

        # stream server from gevent
        self.SERVER = StreamServer(sock, self.connection_handler)

        # current address
        if self.socket_type=="tcp":
            ip, self.port = self.SERVER.address
            self.ip = self.__ip_translate(ip)
            self.address = "tcp://%s:%i" % (self.ip, self.port)

        elif self.socket_type=="ipc":
            self.address = "ipc://%s" % addr

        # serialization
        self.serializer = Serializer()

    def __ip_translate(self, ip):
        """
        Translate interface to ip adress, and stores
        all used ip adresses used to bind socket to
        """
        self.__listening_on = []
        if ip=="127.0.0.1":
            self.__listening_on.append(ip)
            return ip

        if not ip=="0.0.0.0":
            self.__listening_on.append(ip)
            return ip

        # interfaces list
        iflist = all_interfaces()

        # is interface name used?
        if ip in iflist.keys():
            ip = iflist[ip]
            self.__listening_on.append(ip)
            return ip

        # listening on 0.0.0.0 (all interfaces)
        for name, ifip in iflist.items():
            self.__listening_on.append(ifip)

        return ip


    def binded_ip_list(self):
        """
        list of addresses on which is listening socket
        """
        lst = []
        for ip in self.__listening_on:
            lst.append( "tcp://%s:%i" % (ip, self.port) )
        return lst


    def kill(self):
        self.SERVER.kill()


    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        #self.is_running = False
        pass

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
        if message in self._msgdb:
            raise Exception("Message %s is already registered" % message)
        self._msgdb[message]=(func, raw_msg_response)


    def connection_handler(self, SOCK, address):
        ssid = None
        while True:
            try:
                msgdata, resreq = _receive_and_deserialize(SOCK, self.serializer)
            except (NoData, ConnectionClosed):
                return

            try:
                msg = msgdata['message']
            except KeyError:
                if resreq:
                    self._send_noop(SOCK)
                LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
                continue

            # message SET_SESSION_ID is special message
            # it never return reply and is not propagated to handlers
            if msg == messages.SET_SESSION_ID:
                try:
                    ssid = msgdata['id']
                    #print("conn session id" , address, ssid)
                except KeyError:
                    pass
                if resreq:
                    self._send_noop(SOCK)
                continue


            # find message handler
            try:
                handler, rawmsg = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored
                if resreq:
                    self._send_noop(SOCK)
                LOG.warning("Unknown message received [%s]" % msg)
                LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue

            # run handler
            try:
                result = handler(msgdata)
            except Exception as e:
                result = exception_serialize(e, False)
                LOG.info("Exception [%s] when processing message [%s]. Message: %s." % (result['name'], msg, result['description']) )
                #LOG.debug("Message dump: %s" % repr(msgdata) )
                #LOG.debug(result['traceback'])

                if not resreq:
                    # if response is not required, then don't send exceptions
                    continue

                _serialize_and_send(
                    SOCK,
                    self.serializer,
                    exception_serialize(e, False),
                    resreq = False, # response never require another response
                )
                continue

            # response is not expected, throw result and back to loop
            if not resreq:
                continue

            try:
                # send result
                if rawmsg:
                    _serialize_and_send(
                        SOCK,
                        self.serializer,
                        result,
                        resreq = False,
                    )
                else:
                    _serialize_and_send(
                        SOCK,
                        self.serializer, {
                            "message":messages.RESULT,
                            "result":result,
                        },
                        resreq = False
                    )
            except ConnectionClosed:
                return

    def _send_noop(self, SOCK):
        """
        Sends empty message
        """
        _serialize_and_send(
            SOCK,
            self.serializer, {
                "message":messages.NOOP,
            },
            resreq = False
        )


def send_and_receive(address, message, timeout=10):
    """
    address - full destination address (eg: tcp://127.0.0.1:1234)
    message - message payload (will be automatically serialized)
    timeout - time in seconds after which TimeoutError will be raised
    """
    serializer = Serializer() # <-- serializer is a singleton

    typ, addr, so1, so2 = decode_addr(address)
    SOCK = socket.socket(so1,so2)
    SOCK.connect(addr)

    # send...
    _serialize_and_send(SOCK, serializer, message, resreq=True)

    # receive response
    try:
        if timeout is None:
            res, resreq = _receive_and_deserialize(SOCK, serializer)
        else:
            # throw exception after timeout and close socket
            try:
                with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                    res, resreq = _receive_and_deserialize(SOCK, serializer)
            except exceptions.ReponseTimeout:
                raise exceptions.ReponseTimeout("Response timeout")
    finally:
        SOCK.close()
    return res


def send_and_receive_response(address, message, timeout=10):
    """
    j.w. ale dekoduje wynik i go zwraca, lub rzuca otrzymany w wiadomości exception
    """
    result = send_and_receive(address, message, timeout)
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
