#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from time import time

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
_MSG_PING           = "p"
_MSG_BROADCAST      = "bc"
_MSG_HOST_JOIN      = "hj"
_MSG_HOST_LEAVE     = "hl"
_MSG_HOST_DIED      = "hd"
_MSG_FULL_STATE_REQ = "sr"
_MSG_FULL_STATE     = "fs"
_MSG_PAYLOAD        = "hp"


class NetworkSync(object):

    def __init__(self, ID, hostname=None):
        self.ID = ID
        self.hostname = hostname
        self.counter = 1  # main state counter
        self.online = True # always True until close is called
        self.counters = {}
        self.lost_hosts = {}

        self.FULL_SYNC_DELAY = 3

        self._methodmap = {
            _MSG_PING           : self.on_ping,
            _MSG_BROADCAST      : self.on_broadcast,
            _MSG_HOST_JOIN      : self.on_host_join,
            _MSG_HOST_LEAVE     : self.on_host_leave,
            _MSG_HOST_DIED      : self.on_host_died,
            _MSG_FULL_STATE_REQ : self.on_full_sync_request,
            _MSG_FULL_STATE     : self.on_host_full_state,
            _MSG_PAYLOAD        : self.on_incoming_payload,
        }

        # internal switches
        self._disable_forwarding = False  # disable forwarding of messages to other hosts
        self._disable_reping = False      # disable ping with counter after registering new host

        # file with known hosts dump for easy network joining
        self.known_hosts_dump_file = None
        self._dump_sheduled = False

    def start(self):
        khlist = self.load_known_hosts()
        # try to join using previously known hosts
        succ = 0
        if not khlist is None:
            for addr in khlist:
                if self.send_ping(addr, no_fail_report=True):
                    succ += 1
                if succ>2:
                    return
        # if normal join fails, send broadcast
        if succ<1:
            self._broadcast()

    def close(self):
        """
        Unregister self from network.
        """
        if not self.online:
            # already off
            return
        self.dump_known_hosts()
        self.online = False
        self.counter += 1
        self._host_leave_forwarder(self.ID, self.counter)

    # connecting to newtork without broadcast
    def known_hosts(self):
        """
        List of locally known hosts
        """
        return self.counters.keys()

    def dump_known_hosts(self):
        """
        Dumps known hosts to file for broadcastless connection next time
        """
        if self.known_hosts_dump_file is None:
            return
        self._dump_sheduled = False
        with file(self.known_hosts_dump_file,"w") as dumpfile:
            for kh in self.remote_host_list():
                if kh['id']==self.ID:
                    continue
                dumpfile.write( kh['addr'] )
                dumpfile.write( "\n" )

    def load_known_hosts(self):
        """
        Load known hosts from file and return as set()
        """
        if self.known_hosts_dump_file is None:
            return
        res = set()
        try:
            with file(self.known_hosts_dump_file,"r") as dumpfile:
                for ln in dumpfile.readlines():
                    res.add( ln.strip() )
        except IOError:
            pass
        return res

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

        if sid in self.lost_hosts:
            # host is not dead
            del self.lost_hosts[sid]

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
                return
        if not 'host_addr' in msg:
            msg['host_addr'] = sender_addr

        fnc(sender_addr, msg)

        # check sender in delay
        # skip checking for messages about joining and leaving network
        # if message is coming from host wchih is joining/leaving
        if msg['SMSG'] in (_MSG_HOST_JOIN, _MSG_HOST_LEAVE, _MSG_HOST_DIED):
            if sid==msg['host_id']:
                return
        self.delay( self.FULL_SYNC_DELAY/2, self.check_sender, sender_addr, sid )

    # internal send and broadcast functions

    def _broadcast(self, counter=None):
        if counter is None:
            counter = self.counter
        msg = {
            'SMSG' : _MSG_BROADCAST,
            'sender_id': self.ID,
            'counter' : counter,
            'hostname' : self.hostname,
        }
        if self.online:
            self.send_broadcast(msg)

    def _send(self, addr, msg, no_fail_report=False):
        """
        Add sender ID and send message to specified address or list of addresses
        """
        msg['sender_id'] = self.ID
        try:
            self.send_message(addr, msg)
            return True
        except Exception:
            pass
        if not no_fail_report:
            self.report_connection_error(host_addr=addr)
        return False

    # top level logic methods

    def _peer_chooser(self, exclude=()):
        """
        Choose nearest hosts from local database to distribute messages
        """
        # list of known hosts without excluded
        hlst = []
        for h in self.remote_host_list():
            h = h['id']
            if h in exclude:
                continue
            hlst.append( h )

        if len(hlst)<=1:
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

    def is_local_state_actual(self, host_id, remote_counter):
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
        return (counter>=remote_counter) and (counter>0)

    def can_bump_local_state(self, host_id, remote_counter):
        """
        Return true if local counter is only 1 steb behind remote state
        """
        try:
            return (remote_counter - self.counters[host_id][0])==1
        except KeyError:
            return False

    def set_counter(self, host_if, counter):
        """
        Set new value for counters.
        """
        self.counters[host_if] = (counter, time() )

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

        #knownaddr = self.hostid2addr( sender_id )
        #if knownaddr is None:
            # new host!
        #print self.ID,"incoming message from unknown host",sender_id, "registering..."
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

    def host_join(self, host_addr, host_id, counter, hostname=None, sender_id=None, from_broadcast=False):
        """
        Detected new host in network (from broadcast, first connection or passef from other host)
            host_addr - new host address
            host_id - new host id
            counter - new host counter
            hostname - new host name
            sender_id - who is sending message (sender host id)
        """
        if self.is_host_known(host_id):
            # host is known
            if from_broadcast:
                # incoming broadcast can notify about new state of host
                # if we have old state (but not initial=0, then in near future
                # we can check remote host for new state, and update it
                hc = self.counters[host_id][0]
                if hc==0: return
                if hc<counter:
                    # remote host has newer state than we already know
                    # check new state in near future
                    self.delay(self.FULL_SYNC_DELAY*2,
                        self._host_check_is_sync_required, host_id, counter
                    )
            return

        # new host is joined
        # Because we doesn't know current state of new host, we can't just set
        # counter, we need to do full sync first.
        self.set_counter(host_id, 0)
        try:
            del self.lost_hosts[host_id]
        except KeyError:
            pass

        # TODO: check address of new host
        # if it's used by another host, then probably previous host
        # died and new started in place of previous.

        self.remote_host_join(host_id, host_addr, hostname )
        self.delay(
            self.FULL_SYNC_DELAY, # delay in seconds
            self._host_check_is_sync_required, host_id, counter
        )

        # dump new known hosts list
        if not self._dump_sheduled:
            self._dump_sheduled = True
            self.delay( self.FULL_SYNC_DELAY*3, self.dump_known_hosts )

        # after some time send to newly joined host ping
        # with own counter state. This is usefull after
        # reconnecting host which previously loosed connection
        if not self._disable_reping:
            self.delay(
                self.FULL_SYNC_DELAY*2,
                self.send_ping,
                host_addr
            )

        # send notification to neighboors
        if self._disable_forwarding:
            return
        peers = self._peer_chooser( (host_id, sender_id) )
        for p in peers:
            destination = self.hostid2addr(p) # destination host addres
            self.send_host_join(
                destination,
                host_id,
                host_addr,
                counter,
                hostname
            )


    def host_leave(self, host_id, counter, sender_id=None):
        """
        Remote host is shutting down or died unexpectly.
        """
        # if host is already unknown, exit
        if not self.is_host_known(host_id):
            return
        # if curernt counter state is higher, then
        # unregistering is not valid now.
        if self.is_local_state_actual(host_id, counter):
            return
        # unregister and delete host data
        try:
            del self.lost_hosts[host_id]
        except KeyError:
            pass
        self.remote_host_exit(host_id)
        del self.counters[host_id]
        # dump changes
        if not self._dump_sheduled:
            self._dump_sheduled = True
            self.delay( self.FULL_SYNC_DELAY*3, self.dump_known_hosts )
        # notify neighbours
        self._host_leave_forwarder( host_id, counter, sender_id )
    def _host_leave_forwarder(self, host_id, counter, sender_id=None ):
        if self._disable_forwarding: return
        # forward message to neighbours
        for hid in self._peer_chooser( (host_id, sender_id) ):
            self.send_host_leave(
                self.hostid2addr( hid ),   # host address
                host_id,
                counter
            )

    def host_process_complete_state(self, host_id, counter, items):
        """
        Process incoming full state report
        items - list of key/value pairs with host properties/workers
        """
        self.remote_host_reset_state(host_id)
        addr = self.hostid2addr(host_id)
        if addr is None:
            # this host is unknown, skip registering
            return
        self.set_counter(host_id, counter)
        # process all payload items
        for itm in items:
            self.remote_payload_process(host_id, itm)

    def _host_check_is_sync_required(self, host_id, counter):
        """
        Check if we need syncing with remote host, if yes, send request for sync
          host_id - sender is asking for host with host id = hostid
          counter - known counter state
          hostname - incoming hostname (not known in accidental incoming message)
        """
        if self.is_local_state_actual(host_id, counter):
            return
        addr = self.hostid2addr(host_id)
        self.send_full_sync_request(addr)

    def host_died(self, host_id):
        """
        Called when remote host died unexpectly.
        """
        if not self.is_host_known(host_id):
            return
        self.remote_host_exit(host_id)
        del self.counters[host_id]
        self._host_died_forwarder(host_id)
    def _host_died_forwarder(self, host_id, sender_id=None):
        if self._disable_forwarding: return
        for hid in self._peer_chooser( (host_id, sender_id) ):
            self.send_host_died(
                self.hostid2addr( hid ),
                host_id,
            )

    def report_connection_error(self, host_addr=None):
        """
        Call this function if destination address is unavailable.
        This function will try to connect to host again, if connection is also unavailable,
        then host is reported as dead.
        """
        hnfo = self.addr2hostid(host_addr)
        if hnfo is None:
            return
        hid = hnfo['id']
        if hid in self.lost_hosts:
            self.host_died(hid)
            return
        self.lost_hosts[hid]=time()
        self.send_ping(hnfo['addr'])


    # sending local properties to all hosts
    def distribute_change(self, data):
        """
        Add or update local property,
        send changes to all peers in network
        """
        self.counter += 1
        self._host_payload_forwarder(self.ID, self.counter, data)
        #self.remote_payload_process(self.ID, data)

    # remote host changed state
    def host_remote_payload(self, host_id, counter, data, sender_id=None):
        """
        Remote host changed state of own component and sends change.
        """
        if self.is_local_state_actual(host_id, counter):
            return
        # remote state looks tobe more than 1 step forward.
        # this means that we need sull sync!
        if not self.can_bump_local_state(host_id, counter):
            # send request to remote host for full sync
            addr = self.hostid2addr(host_id)
            self.send_full_sync_request(addr)
        else:
            # yes, we can bump local state by 1 and update database
            self.set_counter(host_id, counter)
            # if key is given, we can update state
            self.remote_payload_process(host_id, data)
        # forward payload
        self._host_payload_forwarder(host_id, counter, data, sender_id)

    def _host_payload_forwarder(self, host_id, counter, data, sender_id=None):
        """
        Send host change to neighbours
        """
        if self._disable_forwarding: return
        # forward message to neighbours
        for hid in self._peer_chooser( (host_id, sender_id) ):
            self.send_host_payload(
                self.hostid2addr( hid ),   # host address
                host_id,
                counter,
                data
            )

    # network input / output methods
    def send_ping(self, addr, no_fail_report=False):
        """
        Send ping to host.
        noreports parameter is used to disable broken connection reporting.
        """
        msg = {
            "SMSG"    : _MSG_PING,
            "counter" : self.counter,
        }
        return self._send(addr, msg, no_fail_report=no_fail_report)

    def on_ping(self, addr, msg):
        """
        When received ping, check incoming coutner.
        Maybye sending host has newer state than we know.
        """
        try:
            counter = msg['counter']
        except KeyError:
            return
        # schedule remote host counter check
        self.delay( self.FULL_SYNC_DELAY,
            self._host_check_is_sync_required,
            msg['sender_id'], counter
        )

    # joining network

    def on_broadcast(self, addr, msg):
        """
        Noe host send broadcast on start
        """
        self.host_join(
            msg['host_addr'],
            msg['host_id'],
            msg['counter'],
            msg['hostname'],
            msg['sender_id'],
            from_broadcast=True
        )

    def on_host_join(self, addr, msg):
        """
        New host joined network.
        message fields:
            host_id - id of host which joined network
            counter - host status counter
        """
        self.host_join(msg['host_addr'], msg['host_id'], msg['counter'], msg['hostname'], msg['sender_id'])

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

    # leaving network

    def on_host_leave(self, addr, msg):
        """
        Host leaves network
        """
        self.host_leave( msg['host_id'], msg['counter'], msg['sender_id'] )

    def send_host_leave(self, addr, host_id, counter):
        """
        Notify about leaving network.
        addr - target address
        host_id - leaving host id
        counter - leaving host counter
        """
        msg = {
            "SMSG" : _MSG_HOST_LEAVE,
            "host_id" : host_id,
            "counter" : counter
        }
        self._send(addr, msg)

    def on_host_died(self, addr, msg):
        """
        Host died without leaving network.
        """
        self.host_died( msg['host_id'] )

    def send_host_died(self, addr, hostid):
        msg = {
            "SMSG"    : _MSG_HOST_DIED,
            "host_id" : hostid,
        }
        self._send( addr, msg )

    # host change state

    def on_incoming_payload(self, addr, msg):
        """
        Host changed property
        """
        self.host_remote_payload(
            msg['host_id'],
            msg['counter'],
            msg['data'],
            msg['sender_id']
        )

    def send_host_payload(self, addr, hostid, counter, data):
        """
        Send host property change
        """
        msg = {
            "SMSG"    : _MSG_PAYLOAD,
            "host_id" : hostid,
            "counter" : counter,
            "data"    : data,
        }
        self._send( addr, msg )

    def send_host_counter(self, addr, hostid, counter):
        """
        Sends only known counter of host. Used when detected that remote host holds obsolete state.
        """
        self.send_host_proprty_set(addr, hostid, counter, None)

    # full sync request and report

    def on_full_sync_request(self, addr, msg):
        """
        Incoming request for full host report.
        Result will be transmitted back asynchronously.
        """
        self.send_host_full_state( addr )

    def send_full_sync_request(self, addr):
        """
        Send request for full state report from remote host
        """
        msg = { 'SMSG' : _MSG_FULL_STATE_REQ }
        self._send(addr, msg)

    def on_host_full_state(self, addr, msg):
        """
        Remote host data is sending own full state report.
          - hostid - remote host id
          - addr - remote addess
        """
        hid = msg['host_id']

        # update hostname of host
        self.remote_host_set_hostname(hid, msg['hostname'] )

        # register new hosts known by remote host
        for host in msg['known_hosts']:
            if host['id']==self.ID:
                continue
            self.host_join(
                host['addr'],
                host['id'],
                0,
                host['hostname']
            )

        # check if host state is up to date?
        if self.is_local_state_actual( hid, msg['counter'] ):
            return

        # check if host is currently leaving network
        if msg.get("offline",False):
            self.host_leave(hid)
            return

        # process current state of host
        self.host_process_complete_state( hid, msg['counter'], msg['state'] )

    def send_host_full_state(self, addr):
        """
        Send full state to specified host.
        """
        msg = { 'SMSG' : _MSG_FULL_STATE,
            "host_id"  : self.ID,
            "counter"  : self.counter,
            "hostname" : self.hostname,
        }
        # local workers
        if self.online:
            msg["state"] = self.build_local_state_report()
        else:
            msg["offline"] = True
        # all known hosts
        kh = []
        for h in self.remote_host_list():
            if h["id"]!=self.ID:
                kh.append(h)
        msg['known_hosts'] = kh
        self._send(addr, msg)

    # ABSTRACT METHODS, NEED TO BE OVERWRITTEN IN CHILD CLASSESS

    # network communication

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
        """
        Delay execution of function in bacground
        like gevent.start_later(...)
        """
        raise NotImplementedError

    # host database operations

    def hostid2addr(self, host_id):
        """
        Get address of host by ID
        """
        raise NotImplementedError

    def addr2hostid(self, addr):
        """
        Reverse conversion: get host ID for address
        """
        raise NotImplementedError

    def remote_host_join(self, host_id, host_addr, hostname=None):
        """
        Remote host joined network. Hostname can be unknown.
        """
        raise NotImplementedError

    def remote_host_exit(self, host_id):
        """
        Unregister remote host
        """
        raise NotImplementedError

    def remote_host_list(self):
        """
        Return list of all known hosts.
        """
        raise NotImplementedError

    def remote_host_set_hostname(self, host_id, host_addr, hostname):
        """
        Update hostname of remote host
        """
        raise NotImplementedError

    def remote_host_reset_state(self, host_id):
        """
        Local state of remote host should be reseted to initial state.
        Used before accepting full sync.
        """
        raise NotImplementedError

    def remote_payload_process(self, host_id, data):
        """
        Incoming payload describing ne state of remote host.
        Place for own implementation of data processing
        """
        raise NotImplementedError

    def build_local_state_report(self):
        """
        This function should generate list of messages identical
        to sended by distribute_change method.
        This list should fully describe full host status to make possible
        rebuild full state of localhost on remote network node.
        Result will be remotely executed as series of remote_payload function calls.
        """
        raise NotImplementedError



_WORKER_ADD  = '+w'
_WORKER_DEL  = '-w'
_SERVICE_ADD = '+s'
_SERVICE_DEL = '-s'

class KasayaNetworkSync(NetworkSync):

    def __init__(self, dbinstance, *args, **kwargs):
        global gevent, emit
        import gevent
        from kasaya.core.events import emit
        self.DB = dbinstance
        super(KasayaNetworkSync, self).__init__( *args, **kwargs )

    def delay(self, seconds, func, *args, **kwargs):
        """
        Use gevent.start_later to delay function execution.
        """
        g = gevent.Greenlet(func, *args, **kwargs)
        if not seconds:
            g.start()
        else:
            g.start_later(seconds)

    def hostid2addr(self, ID):
        return self.DB.host_addr_by_id( ID )

    def addr2hostid(self, addr):
        for h in self.DB.host_list():
            if h['addr']==addr:
                return h

    # expand property set/del to registering services and workers
    def remote_host_join(self, host_id, host_addr, hostname):
        self.DB.host_register(host_id, host_addr, hostname )
        emit("host-join", host_id, host_addr, hostname)

    def remote_host_exit(self, host_id):
        self.DB.host_unregister(host_id)
        emit("host-leave", host_id)

    def remote_host_list(self):
        return self.DB.host_list()

    def remote_host_set_hostname(self, host_id, hostname):
        """
        Update hostname of remote host if needed
        """
        hi = self.DB.host_info( host_id )
        if hi['hostname'] is None:
            self.DB.host_set_hostname( host_id, hostname )

    def build_local_state_report(self):
        res = []
        for w in self.DB.worker_list(self.ID):
            msg = {
                'ptype'      : _WORKER_ADD,
                'worker_id'  : w['id'],
                'service'    : w['service'],
                'worker_addr': w['addr'],
            }
            res.append(msg)
        for s in self.DB.service_list(self.ID):
            s['ptype'] = _SERVICE_ADD
            res.append(s)
        return res

    def remote_host_reset_state(self, host_id):
        """
        reset database
        """
        self.DB.host_clean(host_id)

    def remote_payload_process(self, host_id, data):
        """
        Process state change generated by build_local_state_report or sended by distribute_change
        """
        try:
            pt = data['ptype']
        except KeyError:
            return
        if pt==_WORKER_ADD:
            # worker add
            wa = self.worker_addr_process(data['worker_addr'], host_id)
            self.DB.worker_register(host_id, data['worker_id'], data['service'], wa )
        elif pt==_WORKER_DEL:
            # worker delete
            wid = data['worker_id']
            self.DB.worker_unregister(ID=wid)
        elif pt==_SERVICE_ADD:
            # servce add
            self.DB.service_add(host_id, data['service'])
        elif pt==_SERVICE_DEL:
            # service delete
            self.DB.service_del(host_id, data['service'])

    # kasaya single changes
    def local_worker_add(self, worker_id, service_name, worker_addr):
        msg = {
            'ptype'      : _WORKER_ADD,
            'worker_id'  : worker_id,
            'service'    : service_name,
            'worker_addr': worker_addr,
        }
        self.distribute_change(msg)

    def local_worker_del(self, worker_id):
        msg = {
            'ptype'      : _WORKER_DEL,
            'worker_id'  : worker_id,
        }
        self.distribute_change(msg)

    def local_service_add(self, service_name):
        msg = {
            'ptype'      : _SERVICE_ADD,
            'service'    : service_name,
        }
        self.distribute_change(msg)

    def local_service_del(self, service_name):
        msg = {
            'ptype'      : _SERVICE_DEL,
            'service'    : service_name,
        }
        self.distribute_change(msg)


    def worker_addr_process(self, worker_addr, host_id):
        """
        Remote workers doesn't send own IP address, only protocol and port.
        If remote protocol is tcp, and there is no IP, we need to check create
        full address using host IP and worker port
        """
        # worker address
        wproto, waddr = worker_addr.split("//",1)
        waddr, wport = waddr.rsplit(":",1)
        if wproto!="tcp:":
            return worker_addr

        # host address
        hostip = self.hostid2addr(host_id)
        if hostip is None:
            return worker_addr
        proto, addr = hostip.split("//",1)
        addr, port = addr.rsplit(":",1)

        # make new worker address
        return wproto+"//"+addr+":"+wport
