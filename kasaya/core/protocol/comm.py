#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib import LOG
from kasaya.conf import settings
from kasaya.core import exceptions
from kasaya.core.lib.system import all_interfaces
from binascii import hexlify
from .sendrecv import *

import socket
import errno
import sys, os



class SimpleSender(object):

    def __init__(self, address, sessionid=None):
        """
        address - destination address inf format: tcp://ipnumber:port
        sessionid - optional session id which will be send immediately after connection.
        """
        self.__working = False
        self.__sessionid = sessionid
        self._address = address
        self.serializer = Serializer()
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
            self.SOCK = SOCK
            self.__working = True
            if self.__sessionid!=None:
                # send session id if is configured
                msg = messages.message_session_id( self.__sessionid )
                self.send(msg)
            self._on_conn_started()
            return True
        except (socket.error, exceptions.NetworkError) as e:
            self._on_conn_closed()
            return False

    def close(self):
        """
        Close socket and stop reconnecting process
        """
        if self.__working:
            self.SOCK.close()

    def _on_conn_started(self):
        """
        called when connection is started
        """
        self.__working = True
        #emit("sender-conn-started", self._address)

    def _on_conn_closed(self):
        """
        called on lost or closed connection
        """
        self.SOCK = None
        self.__working = False
        #emit("sender-conn-closed", self._address)


    @property
    def is_active(self):
        return self.__working

    # communication

    def send(self, message):
        """
        Sends message to target. Return True if success, False if no connection.
        """
        retries = 2 # one extra try only
        while retries>0:
            # not connected already
            retries-=1
            if not self.__working:
                if not self._connect():
                    continue
            # is connected, try send
            try:
                serialize_and_send(self.SOCK, self.serializer, message, resreq=False)
                return
            except exceptions.NetworkError:
                # connection is broken
                self._on_conn_closed()
                continue
        raise ConnectionClosed("Broken connection")


    def send_and_receive(self, message, timeout=None):
        """
        message - message payload (will be automatically serialized)
        timeout - time in seconds after which TimeoutError will be raised.
                  Note that timeout only for receive response, not for sending.
        """
        retries = 2 # one extra try only
        while retries>0:
            # not connected already
            retries-=1
            if not self.__working:
                if not self._connect():
                    continue
            # is connected, try send
            try:
                serialize_and_send(self.SOCK, self.serializer, message, resreq=True)
            except exceptions.NetworkError:
                # connection is broken
                self._on_conn_closed()
                continue
            # receive response
            try:
                res, resreq = receive_and_deserialize(self.SOCK, self.serializer, timeout)
                return res
            except exceptions.NetworkError:
                # connection is broken
                self._on_conn_closed()
                continue
        raise ConnectionClosed("Broken connection")




class Sender(object):
    """
    Connect to target and send multiple messages with or without response.
    autoreconnect - if True enables autoreconnection process
    sessionid - optional session id which will be send immediately after connection.
    """
    def __init__(self, address, autoreconnect=False, sessionid=None):
        """
        address - destination address inf format: tcp://ipnumber:port
        autoreconnect - will try to connect in background until success if connection fails or is unavailable
        """
        global emit, gevent
        import gevent
        from kasaya.core.events import emit

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
                except exceptions.NetworkError:
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


    # communication


    def send(self, message, timeout=None):
        """
        Sends message to target. Return True if success, False if no connection.
        timeout is not supported yet.
        """
        if not self.__working:
            raise ConnectionClosed
        try:
            serialize_and_send(self.SOCK, self.serializer, message, resreq=False)
        except exceptions.NetworkError:
            self.__broken_connection()  # notify about connection loose
            raise


    def send_and_receive(self, message, timeout=None):
        """
        message - message payload (will be automatically serialized)
        timeout - time in seconds after which TimeoutError will be raised.
                  Note that timeout only for receive response, not for sending.
        """
        # send message
        if not self.__working:
            raise ConnectionClosed
        try:
            serialize_and_send(self.SOCK, self.serializer, message, resreq=True)
        except exceptions.NetworkError:
            self.__broken_connection()  # notify about connection loose
            raise

        # receive response
        if timeout is None:
            res, resreq = receive_and_deserialize(self.SOCK, self.serializer, timeout)
        else:
            # throw exception after timeout and close socket
            try:
                with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                    res, resreq = receive_and_deserialize(self.SOCK, self.serializer)
            except exceptions.ReponseTimeout:
                raise exceptions.ReponseTimeout("Response timeout")

        return res



class MessageLoop(object):
    """
    Message loop is used by workers for receiving incoming messages.
    MessageLoop imports gevent.server.StreamServer class
    """

    def __init__(self, address, maxport=None, backlog=50):
        global emit
        from kasaya.core.events import emit
        from gevent.server import StreamServer
        from gevent import socket

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

        # serialization
        self.serializer = Serializer()

        # stream server from gevent
        self.SERVER = StreamServer(sock, self.connection_handler)

        # current address
        if self.socket_type=="tcp":
            ip, self.port = self.SERVER.address
            self.ip = self.__ip_translate(ip)
            self.address = "tcp://%s:%i" % (self.ip, self.port)

        elif self.socket_type=="ipc":
            self.address = "ipc://%s" % addr



    def __ip_translate(self, ip):
        """
        Translate interface to ip adress, and stores all used ip adresses
        """
        self.__listening_on = []
        if ip=="127.0.0.1":
            self.__listening_on.append(ip)
            return ip

        if not ip=="0.0.0.0":
            self.__listening_on.append(ip)
            return ip

        # interfaces list
        # TODO: use of netifaces should be optional
        # if exists, then use it, if not, skip this point
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
        self.SERVER.stop(0)


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

    def register_message(self, message, func, raw_msg_response=False, replace_handler=False):
        """
        message - handled message type
        func - handler function
        raw_msg_response - True means that function returns complete message,
                           False - result shoult be packed to message outside handler
        """
        if message in self._msgdb:
            if not replace_handler:
                raise Exception("Message %s is already registered" % message)
        self._msgdb[message]=(func, raw_msg_response)


    def connection_handler(self, SOCK, address):
        ssid = None
        while True:
            try:
                msgdata, resreq = receive_and_deserialize(SOCK, self.serializer)
            except (NoData, ConnectionClosed):
                return
            except NotOurMessage:
                continue

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
                #LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue

            # run handler
            try:
                result = handler(address, msgdata)
            except Exception as e:
                #LOG.info("Exception [%s] when processing message [%s]. Message: %s." % (result['name'], msg, result['description']) )
                if not resreq:
                    # if response is not required, then don't send exceptions
                    continue

                serialize_and_send(
                    SOCK,
                    self.serializer,
                    messages.exception2message(e),
                    resreq = False, # response never require another response
                )
                continue

            # response is not expected, throw result and back to loop
            if not resreq:
                continue


            try:
                # send result
                if rawmsg:
                    # raw messages should return properly builded message,
                    # so we send data directly awithout building own message
                    serialize_and_send(
                        SOCK,
                        self.serializer,
                        result,
                        resreq = False,
                    )
                else:
                    serialize_and_send(
                        SOCK,
                        self.serializer,
                        messages.result2message(result),
                        resreq = False
                    )
            except ConnectionClosed:
                return

    def _send_noop(self, SOCK):
        """
        Sends empty message
        """
        serialize_and_send(
            SOCK,
            self.serializer,
            messages.noop_message(),
            resreq = False
        )


class UDPMessageLoop(object):

    def __init__(self, port, ID):
        global emit
        from kasaya.core.events import emit
        from gevent import socket
        self.__is_running = True
        self._msgdb = {}
        self.ID = ID
        self.port = port
        self.SOCK = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.SOCK.bind(('',port))
        self.SOCK.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.serializer = Serializer()

    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        self.__is_running = False

    def close(self):
        self.stop()
        self.SOCK.close()

    def register_message(self, message, func):
        self._msgdb[message]=func

    def loop(self):
        while self.__is_running:
            # receive data
            msgdata, addr = self.SOCK.recvfrom(4096)
            # skip own broadcast messages
            #if addr[0]==self.own_ip:
            #    continue
            # deserialize
            try:
                msgdata, repreq = self.serializer.deserialize(msgdata)
            except NotOurMessage:
                continue
            except Exception:
                LOG.warning("Message from broadcast deserialisation error")
                LOG.debug("Broken message body dump in hex (only first 1024 bytes):\n%s" % msgdata[:1024].encode("hex"))
                continue

            # received own broadcast?
            try:
                if msgdata['__sid__'] == self.ID:
                    continue
            except KeyError:
                continue

            # message type
            try:
                msg = msgdata['message']
            except KeyError:
                LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
                continue
            # find handler
            try:
                handler = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored silently
                LOG.warning("Unknown message received [%s]" % msg)
                LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue
            # run handler
            try:
                handler(addr, msgdata)
            except Exception as e:
                if LOG.level>10:
                    continue
                import traceback
                # log exception details
                excname = e.__class__.__name__
                # traceback
                tback = traceback.format_exc()
                try:
                    tback = unicode(tback, "utf-8")
                except:
                    tback = repr(tback)
                # error message
                errmsg = e.message
                try:
                    errmsg = unicode(errmsg, "utf-8")
                except:
                    errmsg = repr(errmsg)
                # log & clean
                LOG.error("Exception [%s] when processing message [%s]. Message: %s." % (excname, msg, errmsg) )
                LOG.debug("Message dump: %s" % repr(msgdata) )
                LOG.debug(tback)
                del excname, tback, errmsg

    def broadcast_message(self, msg):
        """
        Wysłanie komunikatu do wszystkich odbiorców w sieci
        """
        msg['__sid__'] = self.ID
        msg = self.serializer.serialize(msg, resreq=False)
        try:
            self.SOCK.sendto(msg, ('<broadcast>', self.port) )
            return True
        except socket.error as e:
            return False

