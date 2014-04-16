#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os, random

from kasaya.conf import set_value, settings
from kasaya.core.lib import make_kasaya_id
from kasaya.workers.kasayad.netsync import HostDB, KasayaNetworkSync
import gevent


class KasayaFakeSync(KasayaNetworkSync):

    def __init__(self, testpool, dbinstance, ID):
        self.TP = testpool
        super(KasayaFakeSync, self).__init__( dbinstance, ID)

    # network sync operations will execute KasayaTestPool methds in greenlets
    # to make them asynchronous
    def broadcast(self, hostid, cmajor):
        g = gevent.Greenlet( self.TP.broadcast, hostid, cmajor )
        g.start()



class KasayaTestPool(object):

    def __init__(self):
        self.hosts = {}

    def hosts(self):
        return self.hosts.keys()

    def new_host(self, hid=None):
        if hid is None:
            hid = make_kasaya_id(True)
        db = HostDB()
        h = KasayaFakeSync(self, db, hid)
        self.hosts[hid] = h
        return hid

    # fake network operations
    def broadcast(self, hostid, cmajor):
        print ("broadcast!", hostid, cmajor)



class NetSyncTest(unittest.TestCase):

    #@classmethod
    #def setUpClass(cls):
    #    set_value("KASAYAD_DB_BACKEND", "memory")

    def test_network_syncer(self):
        pool = KasayaTestPool()
        pool.new_host()
        gevent.wait()
        print ("after wait")





if __name__ == '__main__':
    unittest.main()
