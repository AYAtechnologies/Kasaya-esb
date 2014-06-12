#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa/bin/python
from __future__ import division, absolute_import, unicode_literals
import unittest, os, random

from kasaya.conf import set_value, settings
from kasaya.core.lib import make_kasaya_id
from kasaya.workers.kasayad.netsync import KasayaNetworkSync
from kasaya.workers.kasayad.db.netstatedb import NetworkStateDB
import gevent



class KasayaNullSync(KasayaNetworkSync):
    """
    Disabled network communication.
    """
    def send_broadcast(self, msg):
        pass
    def send_message(self, addr, msg):
        pass


class KasayaFakeSync(KasayaNetworkSync):

    def __init__(self, testpool, dbinstance, ID, hostname):
        self.TP = testpool
        super(KasayaFakeSync, self).__init__( dbinstance, ID, hostname)
        self.FULL_SYNC_DELAY = 0.1

    def __repr__(self):
        return "<KS:%s>" % (self.ID)

    def _get_disable_forwarding(self):
        return self.TP.disable_forwarding
    def _set_disable_forwarding(self, value):
        pass
    _disable_forwarding = property(_get_disable_forwarding,_set_disable_forwarding)

    def _get_disable_reping(self):
        return self.TP.disable_reping
    def _set_disable_reping(self, value):
        pass
    _disable_reping = property(_get_disable_reping,_set_disable_reping)

    # redirect network operation to test pool which simulate network operations

    def send_broadcast(self, msg):
        if self.TP.disable_broadcast:
            return
        self.TP.send_broadcast( self.ID, msg )

    def send_message(self, addr, msg):
        if not self.TP.link_accept is None:
            fnc = self.TP.link_accept
            dst = self.TP._get_host_for_ip(addr)
            if not fnc(self.ID, dst.ID, msg['SMSG']):
                raise Exception("connection lost")
        #gevent.sleep()
        #self.TP.send_message( addr, msg )
        g = gevent.Greenlet( self.TP.send_message, addr, msg)
        g.start()


class KasayaTestPool(object):

    def __init__(self):
        self.disable_forwarding = False
        self.disable_reping = False
        self.hosts = {}
        self.__ips = {}  # ip address map
        self.__cnt = 0
        # disable broadcast
        self.disable_broadcast = False
        self.__hl = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        # stats
        self.send_counter = 0
        self.broadcast_counter = 0
        self.link_accept = None

    def status(self):
        print "DISABLED: ",
        print "forwarding", self.disable_forwarding,
        print " reping", self.disable_reping,
        print " broadcast", self.disable_broadcast
        print "COUNTERS:  send", self.send_counter, " broadcast", self.broadcast_counter

    # To be like a dict...
    def keys(self):
        return self.hosts.keys()
    def values(self):
        return self.hosts.values()
    def __len__(self):
        return len(self.hosts)
    def __getitem__(self,k):
        return self.hosts[k]
    def items(self):
        return self.hosts.items()

    def new_host(self, hid=None, dont_start=False):
        if hid is None:
        #    hid = make_kasaya_id(True)
            hid = self.__hl[0]
            self.__hl = self.__hl[1:]
        db = NetworkStateDB()
        self.__cnt += 1
        hn = "host_%i" % self.__cnt
        # new host
        ip = "192.168.%i.%i" % (random.randint(1,254), random.randint(1,254))
        self.__ips[hid] = ip
        h = KasayaFakeSync(self, db, hid, hn)
        # add to pool
        self.hosts[hid] = h
        # start if not disabled
        if not dont_start:
            h.start()
        return hid

    def _get_ip_for_host(self, hid):
        """
        Return fake IP address for host
        """
        return self.__ips[hid]

    def _get_host_for_ip(self, ip):
        for h, i in self.__ips.items():
            if i==ip:
                return self[h]
        raise KeyError("Host with ip %s not found" % ip)

    # fake network operations
    def send_broadcast(self, sender, msg):
        fnc = self.link_accept
        senderaddr = self._get_ip_for_host( msg['sender_id'] )
        self.broadcast_counter += 1
        for host in self.hosts.values():
            # broken network simulation
            if not fnc is None:
                dst = host.ID
                if not fnc(sender, dst, msg['SMSG']):
                    continue
            # send
            g = gevent.Greenlet( host.receive_message, senderaddr, msg )
            g.start()

    def send_message(self, addr, msg):
        senderaddr = self._get_ip_for_host( msg['sender_id'] )
        host = self._get_host_for_ip(addr)
        self.send_counter+=1
        host.receive_message(senderaddr, msg)

    def PKH(self, host):
        """
        Print Known Hosts
        """
        hosts = host.TP.keys()
        hosts.sort()
        print host.ID,"-> [",
        for h in hosts:
            if h in host.known_hosts():
                print h,
            else:
                print " ",
        print "]"

    def PP(self):
        hl = self.keys()
        hl.sort()
        for h in hl:
            self.PKH(self[h])



class NetSyncTest(unittest.TestCase):

    def test_counters(self):
        ns = KasayaNullSync(None, "ownid")
        self.assertEqual( ns.is_local_state_actual("h",  0), False ) # unknown host, alwasy not actual
        ns.set_counter("h", 10 )
        self.assertEqual( ns.is_local_state_actual("h",  9), True  )
        self.assertEqual( ns.is_local_state_actual("h", 11), False )
        # can bump version?
        self.assertEqual( ns.can_bump_local_state("h",10) , False )
        self.assertEqual( ns.can_bump_local_state("h",11) , True )
        self.assertEqual( ns.can_bump_local_state("h",12) , False )
        # is known?
        self.assertEqual( ns.is_host_known("h"), True )
        self.assertEqual( ns.is_host_known("e"), False )

    def test_broadcast(self):
        pool = KasayaTestPool()
        pool.disable_forwarding = True
        pool.disable_broadcast = False
        pool.disable_reping = True
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        gevent.wait() # alow all hosts to synchronize

        # how many broadcasts was sent
        self.assertEqual( pool.broadcast_counter, len(pool) )

        # check all hosts know all others, but not self
        hosts = pool.keys()
        for hid,host in pool.items():
            for h in hosts:
                # we can't use is_local_state_actual method, because after broadcast
                # new host still have "out of sync" state!
                # we check if host is registered in each host local database
                status = h in host.counters.keys()
                #status = host.is_local_state_actual(h, pool[h].counter)
                shouldbe = hid!=pool[h].ID
                self.assertEqual(
                    status, shouldbe,
                    "Host %s, checking status of %s, should be %s" % (host.ID, h, str(shouldbe))
                )

    def test_peer_chooser(self):
        pool = KasayaTestPool()
        pool.disable_broadcast = False
        pool.disable_forwarding = True
        # no other hosts
        A = pool.new_host()
        A = pool[A]
        gevent.wait()
        self.assertEqual( len(A._peer_chooser()), 0 )

        # one neighbour
        B = pool.new_host()
        B = pool[B]
        gevent.wait()
        peers = B._peer_chooser()
        self.assertEqual( len(peers), 1 )
        self.assertIn( A.ID, peers )

        # two neighbours
        C = pool.new_host()
        C = pool[C]
        gevent.wait()
        peers = B._peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertIn( A.ID, peers )
        self.assertIn( C.ID, peers )

        # many neighbours
        D = pool.new_host()
        D = pool[D]
        E = pool.new_host()
        E = pool[E]
        F = pool.new_host()
        F = pool[F]
        gevent.wait()

        # simple choices
        peers = A._peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["F","B"], peers )

        peers = F._peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["A","E"], peers )

        peers = D._peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["C","E"], peers )

        # choices with exclusions
        peers = A._peer_chooser( ["B"] )
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["C","F"], peers )

        peers = A._peer_chooser( ["B","F"] )
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["C","E"], peers )

    def test_inter_host_sync(self):
        gevent.wait()
        pool = KasayaTestPool()
        pool.disable_broadcast = True
        pool.disable_forwarding = True
        pool.disable_reping = True

        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        gevent.wait() # alow all hosts to synchronize
        # before broadcast hosts doesn't know about others
        for hid, host in pool.items():
            self.assertEqual( len( host.known_hosts() ), 0 )
        # send broadcast from one host
        pool.disable_broadcast = False
        bchost = "E"# random.choice(pool.keys())
        bchost = pool[bchost]
        bchost._broadcast()
        # broadcast should result in requests from hosts about new host state
        # after this all hosts should know new host, and new host should know
        # all other hosts.
        gevent.wait()

       # each host should know broadcasting host
        # and broadcasting host should know all others
        for hid, host in pool.items():
            kh = host.known_hosts()

            if (hid!=bchost.ID):
                kh = host.known_hosts()
                self.assertEqual( len(kh), 1 ) # one known host
                self.assertIn( bchost.ID, kh ) # broadcasting one
            else:
                # broadcasting host should know all others
                kh = host.known_hosts()
                #self.assertEqual( len(kh), len(pool)-1, "Broadcasting host should know %i other hosts" % (len(pool)-1) )
                for p in pool.keys():
                    if p==bchost.ID:
                        continue
                    self.assertIn( p, kh, "Broadcasting host %s should know %s host" % (bchost.ID, p) )

        #print pool.send_counter
        # Now we create new host without broadcasting
        pool.disable_broadcast = True
        pool.disable_forwarding = True
        pool.disable_reping = True
        nh = pool.new_host()
        gevent.wait()

        self.assertEqual( len(pool[nh].known_hosts()), 0 )
        # now, we send from new host single message to one random host
        # this should result in cascading host registering by passing
        # information about new host to all hosts in network
        pool.disable_forwarding = False
        peers = list( set(pool.keys())-set(nh) )
        peers.sort()
        target = random.choice( peers )
        # send ping to initiate host registering and p2p messages
        pool[nh].send_ping( pool._get_ip_for_host(target) )
        gevent.wait()
        # after full sync each host should know all other hosts
        for p in peers:
            kh = set(pool.keys())
            myid = pool[p].ID
            kh-=set( (myid,) )
            self.assertEqual( kh, set(pool[p].known_hosts()) )

    def test_host_leave(self):
        pool = KasayaTestPool()
        #pool.disable_forwarding = True
        #pool.disable_broadcast = True
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        gevent.wait()
        # one host is closing...
        exiting_host = random.choice( pool.keys() )
        should_know = set(pool.keys()) - set( (exiting_host,) )
        pool[exiting_host].close()
        gevent.wait()

        # check all other hosts are deregistered exiting host
        for p in pool.keys():
            if p==exiting_host:
                continue
            kh = set(pool[p].known_hosts())
            self.assertEqual( kh, should_know - set([p]) )

    def test_host_change(self):
        pool = KasayaTestPool()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        gevent.wait()
        pool.send_counter = 0

        # one host is closing...
        host = pool[  random.choice(pool.keys())  ]
        waddr = "tcp://123.234.34.45:5001"
        host.local_worker_add("W01", "test", waddr)

        gevent.wait()

        # check if worker known on each host
        for p in pool.keys():
            p = pool[p]
            nfo = p.DB.worker_get("W01")
            if p.ID==host.ID:
                # network sync doesn't register own workers/services.
                # This should be done separately
                self.assertEqual(nfo, None)
            else:
                self.assertEqual(nfo['service'], "test")
                self.assertEqual(nfo['addr'], waddr)
                self.assertEqual(nfo['host_id'], host.ID)

        # unregister worker
        host.local_worker_del("W01")
        gevent.wait()

        # noone host should know worker
        for p in pool.keys():
            p = pool[p]
            nfo = p.DB.worker_get("W01")
            self.assertEqual(nfo, None)

        # let's add host with some workers
        pool.disable_forwarding = True
        pool.disable_broadcast = True
        F = pool.new_host()
        F = pool[F]
        gevent.wait()

        # add worker to database and distribute
        F.DB.worker_register(F.ID, "W02", "test", waddr )
        F.DB.service_add(F.ID, "mailer")
        F.local_worker_add("W02","test", waddr)
        F.local_service_add("mailer")
        pool.disable_forwarding = False
        pool.disable_broadcast = False
        # send broadcast
        F._broadcast()
        gevent.wait()

        # check if all hosts synchronised state
        for p in pool.keys():
            p = pool[p]
            nfo = p.DB.worker_get("W02")
            self.assertEqual(nfo['service'], "test")
            self.assertEqual(nfo['addr'], waddr)
            self.assertEqual(nfo['host_id'], F.ID)
            # check services
            nfo = [ s['service'] for s in p.DB.service_list(F.ID) ]
            self.assertIn( "mailer", nfo )
            self.assertEqual( len(nfo), 1 )

        # remove service
        F.DB.service_del(F.ID, "mailer")
        F.local_service_del( "mailer" )
        gevent.wait()

        # check if all hosts removed service
        for p in pool.keys():
            p = pool[p]
            nfo = [ s['service'] for s in p.DB.service_list(F.ID) ]
            self.assertEqual( len(nfo), 0 )

    def test_single_host_connection_error(self):
        pool = KasayaTestPool()
        pool.new_host()
        pool.new_host()
        pool.new_host() # C
        pool.new_host()
        pool.new_host()
        pool.new_host() # F
        pool.new_host() # G
        gevent.wait()
        # choose 2 failing hosts
        failed1 = pool["C"]
        failed2 = pool["F"]

        def fail_link(src, dst, msgtype):
            """
            transmission with host D is not working
            """
            failed = failed1.ID, failed2.ID
            if src in failed: return False
            if dst in failed: return False
            return True
        pool.link_accept = fail_link
        # new host is joining
        newhost = pool.new_host() # H
        gevent.wait()

        # two failed hosts
        self.assertNotIn( newhost, failed1.known_hosts() )
        self.assertNotIn( newhost, failed2.known_hosts() )

        sholudknow = set( pool.keys() ) - set((failed1.ID, failed2.ID))
        for p in pool.values():
            if p.ID in (failed1.ID, failed2.ID):
                continue
            self.assertItemsEqual( p.known_hosts(), sholudknow-set(p.ID) )

        hA = pool['A']
        hA.DB.worker_register(hA.ID, "W01", "test", "addr_a")
        hA.local_worker_add("W01", "test","addr_a")
        hG = pool['G']
        hG.DB.worker_register(hG.ID, "W02", "test", "addr_g1")
        hG.local_worker_add("W02", "test","addr_g1")
        hG.DB.worker_register(hG.ID, "W03", "test", "addr_g2")
        hG.local_worker_add("W03", "test","addr_g2")
        gevent.wait()

        # all working hosts are synchronized
        for p in pool.values():
            if p.ID in (failed1.ID, failed2.ID):
                continue
            wnfo = p.DB.worker_get("W01")
            self.assertNotEqual(wnfo, None)
            wnfo = p.DB.worker_get("W02")
            self.assertNotEqual(wnfo, None)
            wnfo = p.DB.worker_get("W03")
            self.assertNotEqual(wnfo, None)

        # join back broken hosts and send broadcast
        # this should trigger syncing all previously inactive hosts
        pool.link_accept = None
        hA._broadcast()
        gevent.wait()
        should_know = set( pool.keys() )

        # after old hosts joined network, all hosts should know same workers
        for p in pool.values():
            self.assertItemsEqual( p.known_hosts(), should_know - set(p.ID) )
            wnfo = p.DB.worker_get("W01")
            self.assertEqual(wnfo["id"], "W01" )
            wnfo = p.DB.worker_get("W02")
            self.assertEqual(wnfo["id"], "W02" )
            wnfo = p.DB.worker_get("W03")
            self.assertEqual(wnfo["id"], "W03" )

    def test_network_split(self):
        pool = KasayaTestPool()
        # A,B,C hosts
        pool.new_host()
        pool.new_host()
        pool.new_host()
        # D,E,F hosts
        pool.new_host()
        pool.new_host()
        pool.new_host()
        gevent.wait()

        # simulate network split
        # hosts A,B,C and D,E,F,G can't send messages
        def network_split(src, dst, msgtype):
            """
            drops messages between two halfs of network
            """
            if (src in "ABC") and (dst in "DEFG"):
                return False
            if (src in "DEFG") and (dst in "ABC"):
                return False
            return True
        pool.link_accept = network_split

        # add worker to host
        host = pool["B"]
        waddr = "tcp://196.168.99.100:5000"
        host.DB.worker_register(host.ID, "W01", "test", waddr )
        host.local_worker_add("W01", "test", waddr)
        gevent.wait()

        # check if ABC hosts know only half of network
        for p in "ABC":
            p = pool[p]
            for p2 in "ABC":
                if p.ID==p2:
                    continue
                self.assertEqual( p.is_host_known(p2), True )

            for p2 in "DEF":
                self.assertEqual( p.is_host_known(p2), False )

        # check if all hosts know new worker on B
        for p in "ABC":
            p = pool[p]
            wrkr = p.DB.worker_get("W01")
            self.assertEqual(wrkr['host_id'], host.ID)

        # adding new host to splitted network (to DEF set)
        pool.new_host()
        gevent.wait()
        for p in pool.keys():
            kh = pool[p].known_hosts()
            if p in "ABC":
                self.assertItemsEqual( kh, set("ABC")-set(p) )
            else:
                self.assertItemsEqual( kh, set("DEFG")-set(p) )

        # Joining back splitted network
        pool.link_accept=None

        # send message from host in one half to other half of network
        # from G to A
        pool["G"].send_ping( pool._get_ip_for_host("A") )
        gevent.wait()

        # now all hosts should know all others and should know information about worker W01
        for p in pool.keys():
            p = pool[p]
            self.assertItemsEqual( p.known_hosts(), set(pool.keys()) - set([p.ID]) )
            wrkr = p.DB.worker_get("W01")
            self.assertEqual(wrkr['host_id'], host.ID)

    def test_joining_without_broadcast(self):
        pool = KasayaTestPool()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        pool.new_host()
        gevent.wait()

        dumpname = "/tmp/test_kasaya_dump_file"
        # test dump
        dumphost = pool['A']
        dumphost.known_hosts_dump_file = dumpname
        dumphost.dump_known_hosts()
        f = file(dumpname, "r")
        addresses = set()
        for ln in f.readlines():
            addresses.add( ln.strip() )

        for kh in dumphost.known_hosts():
            self.assertIn( pool._get_ip_for_host(kh), addresses )
        self.assertItemsEqual( dumphost.load_known_hosts(), addresses )

        # test join without broadcast
        pool.disable_broadcast = True
        nh = pool.new_host(dont_start=True)
        nh = pool[nh]
        gevent.wait()

        # set dump file name and start host
        nh.known_hosts_dump_file = dumpname
        nh.start()
        gevent.wait()

        # check if host is knowing all other hosts
        self.assertItemsEqual( nh.known_hosts(),  set( pool.keys() ) - set( nh.ID )  )


if __name__ == '__main__':
    unittest.main()

