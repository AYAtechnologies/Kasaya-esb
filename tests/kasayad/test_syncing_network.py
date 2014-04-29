#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa/bin/python
from __future__ import division, absolute_import, print_function, unicode_literals
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

    def __init__(self, testpool, dbinstance, ID):
        self.TP = testpool
        super(KasayaFakeSync, self).__init__( dbinstance, ID)

    def __repr__(self):
        return "<KS:%s>" % (self.ID[1:])

    # redirect network operation to test pool which simulate network operations

    def send_broadcast(self, msg):
        g = gevent.Greenlet( self.TP.send_broadcast, msg)
        g.start()

    def send_message(self, addr, msg):
        g = gevent.Greenlet( self.TP.send_message, addr, msg)
        g.start()

    def create_full_state_report(self):
        return []


class KasayaTestPool(object):

    def __init__(self):
        self.hosts = {}
        self.__ips = {}  # ip address map

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
            hid = make_kasaya_id(True)
        db = NetworkStateDB()
        h = KasayaFakeSync(self, db, hid)
        self.hosts[hid] = h
        return hid

    def _get_ip_for_host(self, hid):
        """
        Return fake IP address for host
        """
        try:
            return self.__ips[hid]
        except KeyError:
            pass
        ip = "192.168.%i.%i" % (random.randint(1,254), random.randint(1,254))
        self.__ips[hid] = ip
        return ip

    def _get_host_for_ip(self, ip):
        for h, i in self.__ips.items():
            if i==ip:
                return self[h]
        raise KeyError("Host with ip %s not found" % ip)

    # fake network operations
    def send_broadcast(self, msg):
        senderaddr = self._get_ip_for_host( msg['senderid'] )
        for host in self.hosts.values():
            g = gevent.Greenlet( host.receive_message, senderaddr, msg )
            g.start()

    def send_message(self, addr, msg):
        senderaddr = self._get_ip_for_host( msg['senderid'] )
        host = self._get_host_for_ip(addr)
        host.receive_message(senderaddr, msg)


class NetSyncTest(unittest.TestCase):

    #@classmethod
    #def setUpClass(cls):
    #    set_value("KASAYAD_DB_BACKEND", "memory")

    def test_countes(self):
        ns = KasayaNullSync(None, "ownid")
        self.assertEqual( ns.is_local_state_actual("h",  0), False ) # unknown host, alwasy not actual
        ns.set_counter("h", 10 )
        self.assertEqual( ns.is_local_state_actual("h",  9), True  )
        self.assertEqual( ns.is_local_state_actual("h", 11), False )

    def test_network_syncer(self):
        pool = KasayaTestPool()
        pool.new_host()
        pool.new_host()
        #pool.new_host()
        #pool.new_host()
        #pool.new_host()
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




        print ("after wait")






if __name__ == '__main__':
    unittest.main()
