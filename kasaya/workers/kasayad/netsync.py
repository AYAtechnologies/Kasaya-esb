#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from time import time

__all__ = ("KasayaNetworkSync",)


_MSG_BROADCAST  = "bc"
_MSG_HOST_JOIN  = "hj"
_MSG_HOST_LEAVE = "hl"
_MSG_FULL_SYNC_REQUEST = "sr"


class KasayaNetworkSync(object):

    def __init__(self, dbinstance, ID):
        #print "BORN",ID
        self.ID = ID
        self.counter = 1  # main state counter
        self.DB = dbinstance
        self._broadcast(0) # counter=0 means out of sync state
        self.counters = {}

        self._methodmap = {
            _MSG_BROADCAST  : self.on_broadcast,
            _MSG_HOST_JOIN  : self.on_host_join,
            _MSG_HOST_LEAVE : self.on_host_leave,
            _MSG_FULL_SYNC_REQUEST : self.on_full_sync_request,
            #"hl" : self.on_leave,
            #"hc" : self.on_host_change,
            #"hd" : self.on_host_died,
            #"wd" : self.on_worker_died,
            #"sr" : self.on_full_sync_request,
            #"hs" : self.on_host_full_state,
        }


    # send and receive network messages

    def receive_message(self, sender_addr, msg):
        """
        Incoming messages dispatcher
        """
        try:
            fnc = self._methodmap[ msg['SMSG'] ]
        except KeyError:
            # invalid message
            return

        if msg['senderid']==self.ID:
            # ignore own messages
            return

        print ("received message from", sender_addr, msg)
        fnc(sender_addr, msg)

    def _broadcast(self, counter):
        msg = {
            'SMSG' : _MSG_BROADCAST,
            'senderid': self.ID,
            'counter' : counter,
        }
        self.send_broadcast(msg)

    def _send(self, addr, msg):
        """
        Add sender ID and send message
        """
        msg['senderid'] = self.ID
        self.send_message(addr, msg)


    # top level logic methods


    def is_local_state_actual(self, hostid, rc):
        """
        Check counters for given host.
        Result:
            True  - local stored counters are equal or higher
            False - local stored counters are outdated or 0
        """
        try:
            counter, tstamp = self.counters[hostid]
        except KeyError: # unknown host
            return False
        return (counter>=rc) and (counter>0)


    def set_counter(self, hostid, counter):
        """
        Set new value for counters.
        """
        self.counters[hostid] = (counter, time() )


    def host_join(self, addr, hostid, counter):
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
        self.host_full_sync_required(hostid)
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


    def host_full_sync_required(self, hostid):
        """
        Host require full sync, start sync procedure.
        """
        addr = self.DB.host_addr_by_id(hostid)
        self.send_full_sync_request(addr, hostid)



    # network input / output methods


    def on_broadcast(self, addr, msg):
        self.host_join(
            addr,
            msg['senderid'],
            msg['counter']
        )


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

    def send_host_join(self, addr, hostid, counter):
        """
        New host joined network.
            addr - address of ne host
            hostid - new host id
            counter - remote host current counter
        """
        msg = { 'SMSG' : "hj",
            'hostid' : hostid,
            'sender_id' : self.ID,
            'counter' : counter,
        }
        return msg



    def on_host_leave(self, addr, msg):
        """
        Host leaves network
        message type: authoritative
        """
        pass

    def send_host_leave(self, addr):
        """
        Notify about own shutting down
        """
        msg = { 'SMSG' : "hl",
            'hostid':self.ID,
        }
        return msg



    def on_host_change(self, addr, msg):
        """
        Host changed property
        message type: authoritative
        """
        pass

    def send_host_change(self, addr, hostid, counter, name, value):
        """
        Send host property change
        """
        msg = {
            "hostid"  : hostid,
            "counter" : counter,
            "name"    : name,
            "value"   : value,
        }
        return msg



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

    def send_host_died(self, addr, hostid):
        msg = {
            'hostid' : hostid,
        }



    def on_worker_died(self, addr, msg):
        """
        Detected remote worker death.
        message type: non-authoritative
        This message should be received by host on wchih run worker.
            sender_id - who detected worker death and send this message
            worker_id - wchich worker died
        """
        pass

    def send_worker_died(self, addr, hostid, workerid):
        """
        Send information about worker death
        """
        msg = {
            "hostid"   : hostid,
            "workerid" : workerid,
        }
        self._send(addr, msg)



    def on_full_sync_request(self, addr, msg):
        """
        Incoming request for full host report.
        Result will be transmitted back asynchronously.
        """
        #res = {
        #    'host'   : self.ID,
        #    'counter': self.counter,
        #    'state'  : self.create_full_state_report()
        #}
        print ("INCOMING FULL SYNC", addr, msg)

    def send_full_sync_request(self, addr, hostid):
        """
        Create local state report and send to specified host
           hostid - id of host which message is targeting
        """
        msg = { 'SMSG' : _MSG_FULL_SYNC_REQUEST,
            'hostid' : hostid,
        }
        self._send(addr, msg)




    def on_host_full_state(self, hostid, addr):
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


    def send_host_full_state(self, addr):
        """
        Send full state to specified host
        """
        msg = {
            "hostid"  : self.ID,
            "counter" : self.counter,
            "state"   : self.create_full_state_report(),
        }
        return msg


    # abstract methods, need to be overwritten in child classess

    def send_broadcast(self, msg):
        """
        Send broadcast to all hosts in network about self.
          hostid - own host id
          counter - status counter
        """
        raise NotImplementedError

    def send_message(self, addr, msg):
        """
        Send message
        """
        raise NotImplementedError


    def create_full_state_report(self):
        """
        Create host full status report
        """
        raise NotImplementedError

