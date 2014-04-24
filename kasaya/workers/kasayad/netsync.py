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


    # remote host state database


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
        self.host_full_sync(hostid, addr)
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

    def host_full_sync(self, hostid, addr):
        """
        Remote host data is out of sync, send full sync request and process response
          - hostid - remote host id
          - addr - remote addess
        """
        result = self.request_remote_host_state(hostid, addr)
        print result
        return
        if "id" in result:
            # wrong response
            print ("wrong full sync response")
            pass
        else:
            # full status response
            #self.set_full_host_state()
            pass

    def report_own_state(self, hostid):
        """
        This function must return full state of local host and current local counters.
           hostid - expected hostid, if is not equal with own hostid, then return error
                    response with valid own hostid and counters. This response will be
                    treated as broadcast sended after starting new host.
                    Result:
                      "hostid": - own host id
                      "major" : - major counter
                      "minor" : - minor counter
        Result is dict with values:
          "major" - major counter,
          "minor" - minor counter
          "data"  - current state (dict) or None if host is empty doesn't have any workers
        """
        res = {
                "major" : self.major,
                "minor" : self.minor,
        }
        if self.ID!=hostid:
            # expected host id not match own id,
            # remote host has invalid data
            res["id"] = self.ID
            return res
        else:
            # host id is valid, return own state
            res["state"] = self.local_state_report()
            return res

    #def set_full_host_state(self, hostid, cmajor, cminor, payload):
    #    """
    #    set new state of remote host and update counters
    #    """
    #    pass



    # abstract methods, need to be overwritten in child classess

    def broadcast(self, hostid, cmajor, cminor):
        """
        Send broadcast to all hosts in network about self.
          hostid - sender own host id
          cmajor - major counter state
        """
        raise NotImplementedError

    def request_remote_host_state(self, hostid, addr):
        """
        send to remote host request for full state and return response.
          hostid - id of remote host
          addr - address of remote host
        """
        raise NotImplementedError


    def local_state_report(self):
        """
        Return full localhost state
        """
        raise NotImplementedError
