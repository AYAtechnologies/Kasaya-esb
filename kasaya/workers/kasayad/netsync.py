#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from time import time
import gevent

__all__ = ("KasayaNetworkSync",)


"""
Message format:
  SMSG - message type
  sender_id - ID of host which sends message
optional fields (filled by receiving side if not exists)
  host_id - ID of host about which is message
  host_addr - ID of host about which is message
other fields
  hostname - remote host hostname
  counter - last known counter for _MSG_BROADCAST
"""
_MSG_PING       = "p"
_MSG_BROADCAST  = "bc"
_MSG_HOST_JOIN  = "hj"
_MSG_HOST_LEAVE = "hl"
_MSG_FULL_STATE_REQUEST = "sr"
_MSG_FULL_STATE = "fs"


class KasayaNetworkSync(object):

    def __init__(self, dbinstance, ID, hostname=None):
        #print "BORN",ID
        self.ID = ID
        self.hostname = hostname
        self.counter = 1  # main state counter
        self.online = True # always True until close is called
        self.DB = dbinstance
        self._broadcast(0) # counter=0 means out of sync state
        self.counters = {}

        self.FULL_SYNC_DELAY = 0.1  # for test purposes use short delays

        self._methodmap = {
            _MSG_PING       : self.on_ping,
            _MSG_BROADCAST  : self.on_broadcast,
            _MSG_HOST_JOIN  : self.on_host_join,
            _MSG_HOST_LEAVE : self.on_host_leave,
            _MSG_FULL_STATE_REQUEST : self.on_full_sync_request,
            _MSG_FULL_STATE : self.on_host_full_state,
            #"hc" : self.on_host_change,
            #"hd" : self.on_host_died,
            #"wd" : self.on_worker_died,
            #"sr" : self.on_full_sync_request,
            #"hs" : self.on_host_full_state,
        }

        # internal switches
        self._disable_forwarding = False    # disable forwarding of messages to other hosts


    def close(self):
        """
        Unregister self from network.
        """
        self.online = False
        self.counter += 1
        #self.send_host_leave()


    def known_hosts(self):
        return self.counters.keys()

    # send and receive network messages

    def receive_message(self, sender_addr, msg):
        """
        Incoming messages dispatcher.
        - sender_addr - adderss of host which sends us message
        - msg - body of message
        """
        try:
            fnc = self._methodmap[ msg['SMSG'] ]
            sid = msg['sender_id']
        except KeyError:
            # invalid message
            return

        if sid==self.ID:
            # ignoring own messages
            return

        # optional fields are completed by sender data
        if not 'host_id' in msg:
            msg['host_id'] = sid
        else:
            # this is message from other host, about ourself
            # we ignore such messages
            if msg['host_id']==self.ID:
                #print "remote info about us, ignoring"
                return
        if not 'host_addr' in msg:
            msg['host_addr'] = sender_addr

        #print "received message from", sender_addr, msg
        fnc(sender_addr, msg)

        # check sender in delay
        self.delay( None, self.check_sender, sender_addr, sid )


    def _broadcast(self, counter):
        msg = {
            'SMSG' : _MSG_BROADCAST,
            'sender_id': self.ID,
            'counter' : counter,
            'hostname' : self.hostname,
        }
        if self.online:
            self.send_broadcast(msg)

    def _send(self, addr, msg):
        """
        Add sender ID and send message to specified address or list of addresses
        """
        msg['sender_id'] = self.ID
        #if type(addr) in (list, tuple):
        #    for a in addr:
        #        self.send_message(addr, msg)
        #else:
        self.send_message(addr, msg)






    # top level logic methods

    def peer_chooser(self, exclude=()):
        """
        Choose nearest hosts from local database to distribute messages
        """
        #print "IM",self.ID," I KNOW", self.known_hosts()
        # list of known hosts without excluded
        hlst = []
        for h in self.DB.host_list():
            h = h['id']
            if h in exclude:
                continue
            hlst.append( h )

        if len(hlst)<=1:
            #print "   ",hlst
            return set(hlst)

        result = set()
        # check my own position
        hlst.append( self.ID )
        hlst.sort()
        myidx = hlst.index(self.ID)

        # select two sibling hosts in network
        if myidx>0:
            result.add ( hlst[myidx-1] )
        else:
            result.add ( hlst[-1] )
        if myidx<(len(hlst)-1):
            result.add ( hlst[myidx+1] )
        else:
            result.add ( hlst[0] )
        # remove excluded host from result
        return result


    def is_host_known(self, host_id):
        """
        Is host known already?
        """
        return host_id in self.counters


    def is_local_state_actual(self, host_id, rc):
        """
        Check counters for given host.
        Result:
            True  - local stored counters are equal or higher
            False - local stored counters are outdated or 0
        """
        try:
            counter, tstamp = self.counters[host_id]
        except KeyError: # unknown host
            return False
        return (counter>=rc) and (counter>0)


    def set_counter(self, sender_id, counter):
        """
        Set new value for counters.
        """
        self.counters[sender_id] = (counter, time() )


    def check_sender(self, addr, sender_id):
        """
        When receiving message from remote host, we need to check
        sender address and sender id to detect new hosts.
        Newly detected host will be added to database with counter=0
        which result in request of full sync new host.

        parameters:
          - addr - address of remote host
          - sender_id - ID of remote host
        result:`
            True - sender is known
            False - sender was unknown or invalid, new host was registered
        """
        if self.is_host_known(sender_id):
            return True

        #knownaddr = self.DB.host_addr_by_id( sender_id )
        #if knownaddr is None:
            # new host!
        print self.ID,"incoming message from unknown host",sender_id, "registering..."
        self.host_join(addr, sender_id, 0)
        #return False

        # everything is OK
        #if sender_id==knownaddr:
        #    return True

        # known host address is different!
        # something strange is happening.... :(
        # TODO:
        #   detected IP changes of remote host
        #   do something in such case
        return False


    def host_join(self, host_addr, host_id, counter, hostname=None, sender_id=None):
        """
        Detected new host in network (from broadcast, first connection or passef from other host)
            host_addr - new host address
            host_id - new host id
            counter - new host counter
            hostname - new host name
            sender_id - who is sending message (sender host id)
        """
        if self.is_host_known(host_id):
            # we have same data or newer, ignore message
            print self.ID, "already know", host_id, "skipping"
            return
        # new host is joined
        # Because we doesn't know current state of new host, we can't just set
        # counter, we need to do full sync first.
        self.set_counter(host_id, 0)
        self.DB.host_register(host_id, host_addr, hostname )
        self.delay(
            self.FULL_SYNC_DELAY,
            self.host_full_sync_required, host_id
        )
        #self.host_full_sync_required( host_id )
        #print self.ID,"registered new host",host_id

        # send notification to neighboors
        if self._disable_forwarding:
            return
        peers = self.peer_chooser( (host_id, sender_id) )
        for p in peers:
            destination = self.DB.host_addr_by_id( p )   # destination host address
            print "from",self.ID, "to",p," there is new host: ", host_id
            self.send_host_join(
                destination,
                host_id,
                host_addr,
                counter,
                hostname
            )



    def host_leave(self, sender_id):
        """
        Remote host is shutting down or died unexpectly.
        """
        self.DB.host_unregister(sender_id)
        del self.counters[sender_id]


    def host_state_change(self, sender_id, counter, key, data):
        """
        Remote host changed state of own component and sends new state.
        """
        if self.is_local_state_actual(sender_id, counter):
            return
        print ("STATE CHANGE", host, counter, ">>>", key, data)


    def host_complete_state(self, host_id, counter, items):
        """
        Received complete state of remote host.
        items - list of key/value pairs with host properties/workers
        """
        addr = self.DB.host_addr_by_id(host_id)
        if addr is None:
            # this host is unknown, skip registering
            return
        self.set_counter(host_id, counter)
        #self.DB.host_register(host_id, host_addr, hostname )
        #print "NEW STATE"
        #for i in items:
        #    print i


    def create_full_state_report(self):
        """
        Create status report with all workers and services on host
        """
        #workers = self.DB.worker_list(self.ID, only_online=True)
        #services = self.DB.service_list(self.ID)
        res = {
            "hostname":self.hostname,
            "workers":[],
            "services":[]
            }
        return res


    def host_full_sync_required(self, hostid):
        """
        Remote host require full host status. Send report.
          addr - sender address
          hostid - sender is asking for host with host id = hostid
        """
        addr = self.DB.host_addr_by_id(hostid)
        self.send_host_full_state(addr)


    # network input / output methods


    def on_ping(self, addr, msg):
        """
        Does nothing.
        """
        pass

    def on_broadcast(self, addr, msg):
        """
        Noe host send broadcast on start
        """
        self.host_join(
            msg['host_addr'],
            msg['host_id'],
            msg['counter'],
            msg['hostname'],
            msg['sender_id']
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
        self.host_join(msg['host_addr'], msg['host_id'], msg['counter'], msg['hostname'])

    def send_host_join(self, addr, host_id, host_addr, counter, hostname):
        """
        New host joined network.
            host_id - joining host id
            host_addr - joining host address
            counter - joining host current counter
            hostname - joining host name
        """
        msg = { 'SMSG'  : _MSG_HOST_JOIN,
            'host_id'   : host_id,
            'host_addr' : host_addr,
            'hostname'  : hostname,
            'counter'   : counter,
        }
        self._send(addr, msg)



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
        msg = { "SMSG" : "hl",
            "host_id" : self.ID,
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
            "host_id"  : hostid,
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
            "host_id" : hostid,
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
            "host_id"   : hostid,
            "workerid" : workerid,
        }
        self._send(addr, msg)



    def on_full_sync_request(self, addr, msg):
        """
        Incoming request for full host report.
        Result will be transmitted back asynchronously.
        """
        self.host_full_sync_required( msg['host_id'] )

    def send_full_sync_request(self, addr, hostid):
        """
        Create local state report and send to specified host
           hostid - id of host which message is targeting
        """
        msg = { 'SMSG' : _MSG_FULL_STATE_REQUEST,
            "host_id" : hostid,
        }
        self._send(addr, msg)




    def on_host_full_state(self, addr, msg):
        """
        Remote host data is out of sync, send full sync request and process response
          - hostid - remote host id
          - addr - remote addess
        """
        if self.is_local_state_actual( msg['host_id'], msg['counter'] ):
            # we doesn't need any updates
            return

        if 'offline' in msg:
            # host is currently leaving network
            if msg['offline']:
                self.host_leave(msg['host_id'])
                return

        self.host_complete_state( msg['host_id'], msg['counter'], msg['state'] )



    def send_host_full_state(self, addr):
        """
        Send full state to specified host
        """
        msg = { 'SMSG' : _MSG_FULL_STATE,
            "host_id"  : self.ID,
            "counter" : self.counter,
        }
        if self.online:
            msg["state"] = self.create_full_state_report()
        else:
            msg["offline"] = None
        self._send(addr, msg)


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

    def delay(self, seconds, func, *args, **kwargs):
        func(*args, **kwargs)
