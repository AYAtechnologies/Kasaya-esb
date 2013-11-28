#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals
from kasaya.conf import settings
#from gevent import socket
from kasaya.core.protocol import Serializer, messages
from kasaya.core.lib import LOG
#from kasaya.core.exceptions import NotOurMessage
from kasaya.core.events import emit
#import traceback

"""
self start

1. broadcast about host start

Remote hosts

1. received broadcast about host start
2. if host is new, it is registered in database
3. wait random time (1-5 seconds)

5. request host status
6. connect to new host and send all known hosts
7. connect to new host and send all owne workers


Local host start
- broadcast host start (only host id is in packet)


Remote host start
- received broadcast about new host (including remote ip number)
- registering host in local db with received IP
- wait random time (1-5 seconds)
- connect to new host and send known network status


Local worker started / stopped.

- loop over all known hosts
  - send current known network status


Remote worker started / stopped.

- remote host connect and bring new host status


Kasaya daemon
=============
- each kasaya daemon represent own host in network and all workers
- each kasaya has unique ID
- each kasaya can have more than one network adress (it usually bind to 0.0.0.0 address)
- each kasaya is leader for own workers and own state in network without any election


Kasaya daemon boot time activity

1. broadcast own ID

2. all hosts adds new host and his IP in local database
   each host send response to sender with own ID and IP
   new host stores remote hosts and their IP's in own local db

3. new host know all existing hosts in network
   all hosts know new host

5. kasaya iterate over all hosts and:
   - sends own status and informations
   - request current network state
     network state contains:
     - all known kasaya hosts (for comparision with received hosts via broadcast)
     - all workers currently working on host





"""


class Synchronizer(object):
    """
    Network state synchronizing.

    """

    def __init__(self, DB, ID):
        self.ID=ID
        self.DB=DB



    def worker_start(self, ID):
        pass

    def worker_stop(self, ID):
        pass

    def host_start(self, ID):
        pass

    def host_stop(self, ID):
        pass


