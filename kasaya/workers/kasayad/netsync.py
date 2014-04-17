#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals



class KasayaNetworkSync(object):

    def __init__(self, dbinstance, ID):
        #print "BORN",ID
        self.ID = ID
        self.major = 1  # main state counter
        self.minor = 0  # state counter for nonauthoritative messages
        self.DB = dbinstance
        self.broadcast(self.ID, self.major, self.minor)
        self.counters = {}


    def is_local_state_actual(self, hostid, major, minor):
        """
        Check counters for given host.
        Result:
            True  - local stored counters are equal or higher
            False - local stored counters are outdated
        """
        try:
            lmajor, lminor = self.counters[hostid]
        except KeyError: # unknown host
            return False
        if major<lmajor:
            return True
        elif major>lmajor:
            return False
        return minor<=lminor


    def set_counters(self, hostid, major,  minor):
        """
        Set new value for counters.
        """
        self.counters[hostid] = (major, minor)


    def host_join(self, hostid, addr, cmajor, cminor):
        """
        Detected new host in network (from broadcast or first connection)
        """
        #print "incoming broadcast from %s (%s)" % (hostid, addr)
        if self.ID==hostid:
            # ignore own broadcasts
            return
        #print "my id", self.ID, " incoming",hostid, " known",self.counters.keys()
        if self.is_local_state_actual(hostid, cmajor, cminor):
            # we have newer data than sender, ignore
            #print "KNOWN HOST"
            return

        self.set_counters(hostid, cmajor, cminor)
        self.DB.host_register(hostid, addr)
        self.request_full_sync(hostid)
        #print "host %s joined network" % hostid, "known", self.counters.keys()


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

    def broadcast(self, hostid, cmajor, cminor):
        """
        Send broadcast to all hosts in network about self.
          hostid - own host id
          cmajor - major counter state
        """
        pass

