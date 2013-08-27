#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals

# worker join and leave
WORKER_JOIN = "wrkr/up"
WORKER_LEAVE = "wrkr/stop"
# join and leave network by syncd
HOST_JOIN = "host/up"
HOST_LEAVE = "host/stop"
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
ASYNC_CALL = "async" # PROBABLY UNNECCESSARY
RESULT = "call/result" # result of synchronous task
ERROR = "call/error" # exception thrown in task
# null message
NOOP = "noop" # null message