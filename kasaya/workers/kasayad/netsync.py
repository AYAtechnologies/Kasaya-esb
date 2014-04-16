#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals



class HostDB(object):

    def __init__(self):
        self.db = {}

    def host_get_counters(self, hostid):
        passZres

    def host_set_full_sync_flag(self, state):
        """
        Sets flag for host indicating that host is currently in "full sync required" state.
        This means that host is currently out of sync and complede data set is required to make it sync.
          state=True - sync required
          state=False - data synced
        """
        assert type(stat) is bool
        pass



class KasayaNetworkSync(object):

    def __init__(self, dbinstance, ID):
        self.ID = ID
        self.major = 0  # main state counter
        self.minor = 0  # state counter for nonauthoritative messages
        self.DB = dbinstance
        self.broadcast(self.ID, self.major)


    def host_join(self, hostid, addr):
        """
        Detected new host in network (from broadcast or first connection)
        """
        pass

    def host_leave(self, hostid):
        """
        Remote host is shutting down or died unexpectly
        """
        pass

    def host_state_change(self, hostid, cmajor, cminor, key, data):
        """
        Remote host changed state of own component and sends new state
        """
        pass

    def host_full_state(self, hostid, cmajor, cminor, payload):
        """
        Remote host sends full state and new counters.
        """



    # abstract methods, need to be overwritten in child classess

    def request_full_sync(self, hostid):
        """
        Will be called when node is out of sync
        """
        pass

    def broadcast(self, hostid, cmajor):
        """
        Send broadcast to all hosts in network about self.
          hostid - own host id
          cmajor - major counter state
        """
        pass

