#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from time import time


class KasayaNetworkSync(object):

    def __init__(self, dbinstance, ID):
        #print "BORN",ID
        self.ID = ID
        self.counter = 1  # main state counter
        self.DB = dbinstance
        self.broadcast(self.ID, 0) # counter=0 means out of sync state
        self.counters = {}


    def is_local_state_actual(self, hostid, rc):
        """
        Check counters for given host.
        Result:
            True  - local stored counters are equal or higher
            False - local stored counters are outdated
        """
        try:
            counter, tstamp = self.counters[hostid]
        except KeyError: # unknown host
            return False
        #return (counter>0) and (counter>=rc)
        print ( counter,rc)
        return counter>=rc


    def set_counter(self, hostid, counter):
        """
        Set new value for counters.
        """
        self.counters[hostid] = (counter, time() )


    def host_join(self, hostid, addr, counter):
        """
        Detected new host in network (from broadcast or first connection)
        """
        if hostid==self.ID:
            # message about self
            if self.counter>counter:
                # someone sends obsolete data
                # TODO: send back new status
                pass
            return
        if self.is_local_state_actual(hostid, counter):
            # we have same data or newer, ignore message
            return
        # new host is joined
        self.set_counter(hostid, counter)
        self.DB.host_register(hostid, addr)
        self.request_full_sync(hostid, addr)
        #print "host %s joined network" % hostid, "known", self.counters.keys()

    def host_leave(self, hostid):
        """
        Remote host is shutting down or died unexpectly.
        """
        self.DB.host_unregister(hostid)
        del self.counters[hostid]

    def host_state_change(self, hostid, counter, key, data):
        """
        Remote host changed state of own component and sends new state.
        """
        if self.is_local_state_actual(hostid, counter):
            return
        print ("STATE CHANGE", host, counter, ">>>", key, data)


    def host_full_state(self, hostid, counter, payload):
        """
        Remote host sends full state and new counters.
        payload is dict treated as many key,data values in host_state_change method.
        """
        pass


    # incoming network events

    def on_host_join(self, addr, msg):
        """
        New host joined network.
        message type: authoritative

        msg - message body
        addr - sender address
        message fields:
            sender_id - sending host
            host_id - id of host which joined network
            counter - host status counter
        """
        if msg['sender_id']==self.ID:
            # own message, ignoring
            return
        self.host_join(msg['host_id'], addr, msg['counter'])

    def on_host_leave(self, addr, msg):
        """
        Host leaves network
        message type: authoritative
        """
        pass

    def on_host_change(self, addr, msg):
        """
        Host changed property
        message type: authoritative
        """
        pass

    def on_host_died(self, addr, msg):
        """
        Host died without leaving network.
        message type: non-authoritative

        Message properties:
            sender_id - sending host
            host_id - id of host wchih died
            counter - last known host counter
        if local stored counter is lower or
        """
        pass

    def on_worker_died(self, addr, msg):
        """
        Detected remote worker death.
        message type: non-authoritative
        This message should be received by host on wchih run worker.
            sender_id - who detected worker death and send this message
            worker_id - wchich worker died
        """
        pass

    def on_full_sync_request(self, addr, msg):
        """
        Incoming request for full host report.
        Result will be transmitted back asynchronously.
        """
        res = {
            'host'   : self.ID,
            'counter': self.counter,
            'state'  : self.create_full_state_report()
        }
        print ("INCOMING FULL SYNC", addr, msg)



    # abstract methods, need to be overwritten in child classess

    def create_full_state_report(self):
        """
        Create host full status report
        """
        return {}

   def request_full_sync(self, hostid, addr):
        """
        Will be called when node is out of sync/
            hostid - remote host ID
            addr - address of remote host
        """
        pass

    def send_sync_request(self, addr):
        pass

    def broadcast(self, hostid, counter):
        """
        Send broadcast to all hosts in network about self.
          hostid - own host id
          counter - status counter
        """
        pass

