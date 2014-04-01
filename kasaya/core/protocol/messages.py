#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core import exceptions
import traceback, sys


# message types
SET_SESSION_ID = "ssid/set"
# worker join and leave
WORKER_LIVE = "wrkr/up"
WORKER_LEAVE = "wrkr/stop"

# join and leave network by syncd
HOST_JOIN = "host/up"
HOST_LEAVE = "host/stop"
HOST_REFRESH = "host/refr"

# local heartbeat messages
PING = "wrkr/ping"
PONG = "wrkr/pong"

# local worker <--> syncd dialog
QUERY = "sync/query"
QUERY_MULTI = "sync/mulquery"
WORKER_ADDR = "wrkr/addr"
WORKER_REREG = "wrkr/rereg" # requet worker to register again
CTL_CALL = "wrkr/ctrl" # internal service bus control request

# normal client <--> worker messages
SYNC_CALL = "call/sync" # synchronously call worker taks
ASYNC_CALL = "call/async" # asynchronously call worker taks
#MIDDLEWARE_CALL = "call/mdl" # call wich allows to process middleware part of the message
#SYSTEM_CALL = "call/sys" # call wich allows to process all of the message used in async
RESULT = "call/result" # result of synchronous task
ERROR = "call/error" # exception thrown in task

# null message
NOOP = "noop" # null message


# message building and decoding

def noop_message(result):
    return {
        "message":NOOP,
    }

def result2message(result):
    """
    Convert task result into message format
    """
    return {
        'message' : RESULT,
        'result' : result
    }

# status messages

def message_session_id(self, sid):
    return {
        "message" : SET_SESSION_ID,
        "id" : self.__sessionid,
    }


# exceptions

def exception2message(exc, internal=None):
    """
    Serialize exception object into message.
    """
    if hasattr(exc, 'remote'):
        remote = exc.remote
    else:
        remote = False

    result = {
        'message': ERROR,
        'remote': remote,
    }

    if remote:
        # remote exception (contain additional information)
        result['traceback'] = exc.traceback
        result['description'] = exc.message
        result['name'] = exc.name
        result['request_path'] = exc.request_path
    else:
        # local exceptions
        result['request_path'] = []
        # extract traceback
        if hasattr(exc, 'traceback'):
            tb = exc.traceback
        else:
            tb = traceback.format_exc()

        if sys.version_info<(3,0):
            # python 2
            if type(tb)==str:
                try:
                    tb = unicode(tb,"utf-8")
                except:
                    pass

            # error message
            errmsg = exc.message
            try:
                errmsg = unicode(errmsg, "utf-8")
            except:
                errmsg = errmsg
        else:
            # python 3
            errmsg = str(exc)

        result['traceback'] = tb
        result['description'] = errmsg
        result['name'] = exc.__class__.__name__

    if internal is None:
        # try to guess if exception is servicebus internal exception,
        # or client code exception
        result['internal'] = isinstance(exc, exceptions.ServiceBusException)
    else:
        result['internal'] = internal

    return result



def internal_exception2message(description):
    """
    Simple internal errors serializer
    """
    try:
        remote = exc.remote
    except AttributeError:
        remote = False
    return {
        "message" : messages.ERROR,
        "description" : description,
        "internal" : True,
        "remote" : remote,
        "request_path" : [],
    }


def message2exception(msg):
    """
    Deserialize exception from message into exception object which can be raised.
    """
    #if msg['internal']:
    #else:
    #    e = Exception(msg['description'])
    e = exceptions.RemoteException(msg['description'])
    e.internal = msg['internal']
    # deserialized exception is always remote
    e.remote = True
    # request path
    e.request_path = msg['request_path']
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


