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

    def __repr__(self):
        return "<KS:%s>" % (self.ID[1:])

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

    def delay(self, seconds, func, *args, **kwargs):
        g = gevent.Greenlet(func, *args, **kwargs)
        if not seconds:
            g.start()
        else:
            g.start_later(seconds)



class KasayaTestPool(object):

    def __init__(self):
        self.disable_forwarding = False
        self.hosts = {}
        self.__ips = {}  # ip address map
        self.__cnt = 0
        # disable broadcast
        self.disable_broadcast = False
        self.__hl = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

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
        for host in self.hosts.values():
            g = gevent.Greenlet( host.receive_message, senderaddr, msg )
            g.start()

    def send_message(self, addr, msg):
        senderaddr = self._get_ip_for_host( msg['sender_id'] )
        host = self._get_host_for_ip(addr)
        host.receive_message(senderaddr, msg)



class NetSyncTest(unittest.TestCase):

    #@classmethod
    #def setUpClass(cls):
    #    set_value("KASAYAD_DB_BACKEND", "memory")

    def _test_counters(self):
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

    def _test_peer_chooser(self):
        pool = KasayaTestPool()
        pool.disable_broadcast = False
        pool.disable_forwarding = True
        # no other hosts
        A = pool.new_host()
        A = pool[A]
        gevent.wait()
        self.assertEqual( len(A.peer_chooser()), 0 )

        # one neighbour
        B = pool.new_host()
        B = pool[B]
        gevent.wait()
        peers = B.peer_chooser()
        self.assertEqual( len(peers), 1 )
        self.assertIn( A.ID, peers )

        # two neighbours
        C = pool.new_host()
        C = pool[C]
        gevent.wait()
        peers = B.peer_chooser()
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
        peers = A.peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["F","B"], peers )

        peers = F.peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["A","E"], peers )

        peers = D.peer_chooser()
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["C","E"], peers )

        # choices with exclusions
        peers = A.peer_chooser( ["B"] )
        self.assertEqual( len(peers), 2 )
        self.assertItemsEqual( ["C","F"], peers )

        peers = A.peer_chooser( ["B","F"] )
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
        bchost = random.choice(pool.keys())
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
                self.assertEqual( len(kh), len(pool)-1, "Broadcasting host should know %i other hosts" % (len(pool)-1) )
                for p in pool.keys():
                    if p==bchost.ID:
                        continue
                    self.assertIn( p, kh, "Broadcasting host should know %s host" % p)

        # Now we create new host without broadcasting
        pool.disable_broadcast = True
        pool.disable_forwarding = True
        nh = pool.new_host()
        print "-"*30
        print "CREATED:",nh
        gevent.wait()
        self.assertEqual( len(pool[nh].known_hosts()), 0 )
        # now, we send from new host single message to one random host
        # this should result in cascading host registering by passing
        # information about new host to all hosts in network
        pool.disable_forwarding = False
        peers = list( set(pool.keys())-set(nh) )
        peers.sort()
        target = "D"#random.choice( peers )

        _pk = pool.keys()
        _pk.sort()
        print "All peers", _pk

        target = pool[target]
        msg = {"SMSG":"p", "sender_id":nh}
        print "Sending single message to", target.ID
        target.receive_message(pool._get_ip_for_host(nh), msg)
        gevent.wait()
        print

        for p in peers:
            self.assertIn(nh, pool[p].known_hosts(), "Host %s should know %s" % (p, nh) )



if __name__ == '__main__':
    unittest.main()
