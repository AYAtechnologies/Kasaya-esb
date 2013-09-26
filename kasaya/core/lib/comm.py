#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.protocol import serialize, deserialize, messages
from kasaya.core.lib import LOG
from kasaya.core import exceptions
import zmq.green as zmq
import gevent
import traceback, sys
from binascii import hexlify



class BaseLoop(object):

    def __init__(self, connector, context=None):
        self.is_running = True
        self._msgdb = {}
        if context is None:
            self.__context = zmq.Context()
        else:
            self.__context = context
        # bind to socket
        self.SOCK, addr = connector(self.__context)
        self.address = addr
        addr = addr.split(":")
        if len(addr)==3:
            self.ip = addr[1].lstrip("/")
            self.port = int(addr[2])

    def get_context(self):
        return self.__context

    def stop(self):
        """
        Request warm stop, exits loop after finishing current task
        """
        self.is_running = False

    def close(self):
        self.SOCK.close()

    def register_message(self, message, func, raw_msg_response=False):
        """
            message - handled message type
            func - handler function
            raw_msg_response - True means that function returns complete message,
                               False - result shoult be packed to message outside handler
        """
        self._msgdb[message]=(func, raw_msg_response)

    def loop(self):
        raise NotImplemented("loop method must be implemented when BaseLoop is inherited")



class PullLoop(BaseLoop):
    """
    PullLoop is receiving only loop for messages.
    """

    def loop(self):
        while self.is_running:
            # receive data
            msgdata = self.SOCK.recv()

            # deserialize
            try:
                msgdata = deserialize(msgdata)
                msg = msgdata['message']
            except Exception as e:
                continue

            # find handler
            try:
                handler = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored silently
                continue

            # run handler
            try:
                handler(msgdata)
            except Exception as e:
                # ignore exceptions
                continue




class RepLoop(BaseLoop):
    """
    pętla nasłuchująca na sockecie typu REP (odpowiedzi na REQ).
    """

    def send_noop(self):
        noop = {"message":messages.NOOP}
        self.SOCK.send( serialize(noop) )

    def send(self, message):
        try:
            packet = serialize(message)
        except exceptions.SerializationError as e:
            try:
                packet = exception_serialize_internal( str(e) )
                packet = serialize(packet)
            except:
                self.send_noop()
                return
        self.SOCK.send( packet )


    def loop(self):
        while self.is_running:
            # receive data
            msgdata = self.SOCK.recv()
            # deserialize
            try:
                msgdata = deserialize(msgdata)

            except exceptions.NotOurMessage:
                # not our servicebus message
                self.SOCK.send(b"")
                continue

            except Exception as e:
                self.send_noop()
                LOG.warning("Message deserialisation error")
                LOG.debug("Broken message body dump in hex (only first 1024 bytes):\n%s" % hexlify(msgdata[:1024]))
                continue

            try:
                msg = msgdata['message']
            except KeyError:
                LOG.debug("Decoded message is incomplete. Message dump: %s" % repr(msgdata) )
                continue

            # find handler
            try:
                handler, rawmsg = self._msgdb[ msg ]
            except KeyError:
                # unknown messages are ignored
                self.send_noop()
                LOG.warning("Unknown message received [%s]" % msg)
                LOG.debug("Message body dump:\n%s" % repr(msgdata) )
                continue

            # run handler
            try:
                result = handler(msgdata)
            except Exception as e:
                result = exception_serialize(e, False)
                LOG.info("Exception [%s] when processing message [%s]. Message: %s." % (result['name'], msg, result['description']) )
                LOG.debug("Message dump: %s" % repr(msgdata) )
                LOG.debug(result['traceback'])
                self.send(result)
                continue

            # send result
            if rawmsg:
                self.send( result )
            else:
                self.send( {"message":messages.RESULT, "result":result } )


def send_and_receive(context, address, message, timeout=10):
    """
    context - ZMQ context
    address - full ZMQ destination address (eg: tcp://127.0.0.1:1234)
    message - message payload (will be automatically serialized)
    timeout - time in seconds after which TimeoutError will be raised
    """
    SOCK = context.socket(zmq.REQ)
    SOCK.connect(address)
    SOCK.send( serialize(message) )
    if timeout is None:
        # don't use timeout
        # it's dangerous, can lock client permanently
        # if sender die before sending any response
        res = SOCK.recv()
    else:
        # throw exception after timeout and close socket
        try:
            with gevent.Timeout(timeout, exceptions.ReponseTimeout):
                res = SOCK.recv()
        except exceptions.ReponseTimeout:
            SOCK.close()
            raise exceptions.ReponseTimeout("Response timeout")
    res = deserialize(res)
    SOCK.close()
    return res


def send_and_receive_response(context, address, message, timeout=10):
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
