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

    # redirect network operation to test pool which simulate network operations

    def send_broadcast(self, msg):
        if self.TP.disable_broadcast:
            return
        g = gevent.Greenlet( self.TP.send_broadcast, msg)
        g.start()

    def send_message(self, addr, msg):
        g = gevent.Greenlet( self.TP.send_message, addr, msg)
        g.start()

    def remote_property_set(self, host_id, key, data):
        self
        print self.ID, "remote set", host_id, key, data

    def remote_property_delete(self, host_id, key, data):
        print self.ID, "remote delete", host_id, key


class KasayaTestPool(object):

    def __init__(self):
        self.disable_forwarding = False
        self.hosts = {}
        self.__ips = {}  # ip address map
        self.__cnt = 0
        # disable broadcast
        self.disable_broadcast = False
        self.__hl = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        # stats
        self.send_counter = 0
        self.broadcast_counter = 0


    # be like dict
    def keys(self):
        return self.hosts.keys()
    def __len__(self):
        return len(self.hosts)
    def __getitem__(self,k):
        return self.hosts[k]
    def items(self):
        return self.hosts.items()


    def new_host(self, hid=None):
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
        #h._my_pub_ip = ip
        self.hosts[hid] = h
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
    def send_broadcast(self, msg):
        senderaddr = self._get_ip_for_host( msg['sender_id'] )
        self.broadcast_counter += 1
        for host in self.hosts.values():
            g = gevent.Greenlet( host.receive_message, senderaddr, msg )
            g.start()

    def send_message(self, addr, msg):
        senderaddr = self._get_ip_for_host( msg['sender_id'] )
        host = self._get_host_for_ip(addr)
        self.send_counter+=1
        host.receive_message(senderaddr, msg)



class NetSyncTest(unittest.TestCase):

    def test_counters(self):
        ns = KasayaNullSync(None, "ownid")
        self.assertEqual( ns.is_local_state_actual("h",  0), False ) # unknown host, alwasy not actual
        ns.set_counter("h", 10 )
        self.assertEqual( ns.is_local_state_actual("h",  9), True  )
        self.assertEqual( ns.is_local_state_actual("h", 11), False )

    def test_broadcast(self):
        pool = KasayaTestPool()
        pool.disable_forwarding = True # don't use forwarding
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
        pool = KasayaTestPool()
        # silent host creation (without broadcast and forwarding info)
        pool.disable_forwarding = True
        pool.disable_broadcast = True
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
        bchost = random.choice("C")#pool.keys())
        bchost = pool[bchost]
        bchost._broadcast(0)
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
                self.assertIn( bchost.ID, kh ) # broadca`sting one
            else:
                # broadcasting host should know all others
                kh = host.known_hosts()
                #self.assertEqual( len(kh), len(pool)-1, "Broadcasting host should know %i other hosts" % (len(pool)-1) )
                for p in pool.keys():
                    if p==bchost.ID:
                        continue
                    self.assertIn( p, kh, "Broadcasting host should know %s host" % p)

        # Now we create new host without broadcasting
        pool.disable_broadcast = True
        pool.disable_forwarding = True
        nh = pool.new_host()
        #print "-"*30
        #print "CREATED:",nh
        #print "KNOWN POOLS",pool.keys()
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
        print host
        host.distribute_payload( {'fululu':'umcykcyk','color':4} )
        gevent.wait()




        print "sends", pool.send_counter
        #print changing_host


        #should_know = set(pool.keys()) - set( (exiting_host,) )
        #pool[exiting_host].close()
        #gevent.wait()

        # check all other hosts are deregistered exiting host
        #for p in pool.keys():
        #    if p==exiting_host:
        #        continue
        #    kh = set(pool[p].known_hosts())
        #    self.assertEqual( kh, should_know - set([p]) )


if __name__ == '__main__':
    unittest.main()
