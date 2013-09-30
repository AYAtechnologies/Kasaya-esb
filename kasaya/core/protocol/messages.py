#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals

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
WORKER_ADDR = "wrkr/addr"
WORKER_REREG = "wrkr/rereg" # requet worker to register again
CTL_CALL = "wrkr/ctrl" # internal service bus control request
# normal client <--> worker messages
SYNC_CALL = "call/sync" # synchronously call worker taks
ASYNC_CALL = "call/async" # asynchronously call worker taks
MIDDLEWARE_CALL = "call/mdl" # call wich allows to process middleware part of the message
SYSTEM_CALL = "call/sys" # call wich allows to process all of the message used in async
RESULT = "call/result" # result of synchronous task
ERROR = "call/error" # exception thrown in task
# null message
NOOP = "noop" # null message

