#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os, random

from kasaya.conf import set_value, settings
from kasaya.core.lib import make_kasaya_id
from kasaya.workers.kasayad.netsync import KasayaNetworkSync
from kasaya.workers.kasayad.db.netstatedb import NetworkStateDB
import gevent



class KasayaNullSync(KasayaNetworkSync):
    """
    disabled broadcast on setup, for testing single methods
    """
    def broadcast(self, hostid, cmajor, cminor):
        pass


class KasayaFakeSync(KasayaNetworkSync):

    def __init__(self, testpool, dbinstance, ID):
        self.TP = testpool
        super(KasayaFakeSync, self).__init__( dbinstance, ID)

    # redirect network operation to test pool which simulate network operations

    def broadcast(self, hostid, cmajor, cminor):
        g = gevent.Greenlet( self.TP.broadcast, hostid, cmajor, cminor)
        g.start()

    def request_remote_host_state(self, hostid, addr):
        return self.TP.request_remote_host_state( hostid, addr )

    def local_state_report(self):
        return {}



class KasayaTestPool(object):

    def __init__(self):
        self.hosts = {}
        self.__ips = {}  # ip address map

    def keys(self):
        return self.hosts.keys()

    def new_host(self, hid=None):
        if hid is None:
            hid = make_kasaya_id(True)
        db = NetworkStateDB()
        h = KasayaFakeSync(self, db, hid)
        self.hosts[hid] = h
        return hid

    def get_host_ip(self, hid):
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

    # fake network operations
    def broadcast(self, hostid, cmajor, cminor):
        """
        Simulate broadcasting to all hosts
        """
        ip = self.get_host_ip(hostid)
        for hid, host in self.hosts.iteritems():
            g = gevent.Greenlet( host.host_join, hostid, ip, cmajor, cminor )
            g.start()

    def request_remote_host_state(self, hostid, addr):
        """
        full host state request to remote host
        """
        h = self[hostid]
        r = h.report_own_state(hostid)
        return r



    def __len__(self):
        return len(self.hosts)
    def __getitem__(self,k):
        return self.hosts[k]
    def items(self):
        return self.hosts.items()



class NetSyncTest(unittest.TestCase):

    #@classmethod
    #def setUpClass(cls):
    #    set_value("KASAYAD_DB_BACKEND", "memory")

    def _test_countes(self):
        ns = KasayaNetworkSync(None, "ownid")
        self.assertEqual( ns.is_local_state_actual("h",  0, 0), False ) # unknown host, alwasy not actual
        ns.set_counters("h", 10, 5 )
        self.assertEqual( ns.is_local_state_actual("h",  9, 0), True  )
        self.assertEqual( ns.is_local_state_actual("h", 11, 0), False )
        self.assertEqual( ns.is_local_state_actual("h", 10, 4), True  )
        self.assertEqual( ns.is_local_state_actual("h", 10, 5), True  )
        self.assertEqual( ns.is_local_state_actual("h", 10, 6), False )

    def test_network_syncer(self):
        pool = KasayaTestPool()
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
                status = host.is_local_state_actual(h, pool[h].major, pool[h].minor)
                shouldbe = hid!=pool[h].ID
                self.assertEqual(
                    status, shouldbe,
                    "Host %s, checking status of %s, should be %s" % (host.ID, h, str(shouldbe))
                )

        print ("after wait")






if __name__ == '__main__':
    unittest.main()
