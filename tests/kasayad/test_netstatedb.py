#!/home/moozg/venvs/kasa/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasatest/bin/python
from __future__ import division, absolute_import, unicode_literals
import unittest, os, random
# misc
from kasaya.conf import set_value, settings

from kasaya.workers.kasayad.db.netstatedb import NetworkStateDB
from kasaya.core.lib import make_kasaya_id


class NetstateDBTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        set_value("KASAYAD_DB_BACKEND", "memory")

    def test_host_up_and_down(self):
        DB = NetworkStateDB()
        ID, addr = make_kasaya_id(True), "tcp://192.168.1.2:4000"

        # adding host
        res = DB.host_register(ID, addr)
        self.assertEqual(res, True)
        res = DB.host_register(ID, addr)
        self.assertEqual(res, False)

        # listing hosts
        lst = list( DB.host_list() )
        self.assertEqual( len(lst), 1 )
        self.assertEqual( lst[0]['id'], ID )
        self.assertEqual( lst[0]['hostname'], None )
        self.assertEqual( lst[0]['addr'], addr )

        # get host by ID
        h = DB.host_addr_by_id(ID)
        self.assertEqual( h, addr )

        # unregister
        DB.host_unregister(ID)

        # check unregistering result
        lst = list( DB.host_list() )
        self.assertEqual( len(lst), 0 )
        h = DB.host_addr_by_id(ID)
        self.assertEqual( h, None )

        # register 10 hosts
        for n in range(10,20):
            ID, hn, addr = make_kasaya_id(True), "host_%i" % n, "tcp://192.168.1.%i:4000" % n
            res = DB.host_register(ID, addr)
            self.assertEqual(res, True)
            res = DB.host_register(ID, addr)
            self.assertEqual(res, False)

        # is there 10 hosts on list?
        lst = list( DB.host_list() )
        self.assertEqual( len(lst), 10 )

        # check host query
        for n in range(10,20):
            DB.host_addr_by_id(ID)
        DB.close()

    def assertDictEequal(self, d1, d2):
        self.assertItemsEqual( d1.keys(), d2.keys(), "Keys for dicts not equal. %r != %r" % (d1.keys(), d2.keys()) )
        for k,v in d1.items():
            self.assertEqual( type(v), type(d2[k]), "Types [%r] not equal: %s != %s" % (k, type(v), type(d2[k])) )
            self.assertEqual( v, d2[k], "Value [%r] not equal: %r != %r" % (k, v, d2[k]) )

    def test_worker_registers(self):
        DB = NetworkStateDB()
        host_id, addr = make_kasaya_id(True), "tcp://127.0.0.1:4000"
        DB.host_register(host_id, addr)

        wid1, svc1, addr1 = make_kasaya_id(), "srvc1", "tcp://127.0.0.1:5000"
        wid2, svc2, addr2 = make_kasaya_id(), "srvc2", "tcp://127.0.0.1:5001"
        wid3, svc3, addr3 = make_kasaya_id(), "srvc3", "tcp://127.0.0.1:5002"

        DB.worker_register( host_id, wid1, svc1, addr1, 100, online=True )
        DB.worker_register( host_id, wid2, svc2, addr2, 101, online=False )
        DB.worker_register( host_id, wid3, svc3, addr3 )

        self.assertDictEequal(
            DB.worker_get( wid1 ),
            { 'id'      : wid1,
              'host_id' : host_id,
              'service' : svc1,
              'addr'    : addr1,
              'pid'     : 100,
              'online'  : True })

        self.assertDictEequal(
            DB.worker_get( wid2 ),
            { 'id'      : wid2,
              'host_id' : host_id,
              'service' : svc2,
              'addr'    : addr2,
              'pid'     : 101,
              'online'  : False })

        self.assertDictEequal(
            DB.worker_get( wid3 ),
            { 'id'      : wid3,
              'host_id' : host_id,
              'service' : svc3,
              'addr'    : addr3,
              'pid'     : -1,
              'online'  : True })

        # selecting only online workers for service
        self.assertEqual( DB.choose_worker_for_service(svc1)['id'], wid1 )
        self.assertEqual( DB.choose_worker_for_service(svc2), None )
        self.assertEqual( DB.choose_worker_for_service(svc3)['id'], wid3 )

        # change worker state
        DB.worker_set_state( wid2, True )
        self.assertEqual( DB.choose_worker_for_service(svc2)['id'], wid2 )
        DB.worker_set_state( wid1, False )
        self.assertEqual( DB.choose_worker_for_service(svc1), None )

    def test_host_with_workers(self):
        _hosts = []

        DB = NetworkStateDB()
        svce = ("mail","sms","bubbles","spam")
        # create hosts
        for n in range(6):
            ip = '192.168.1.%i' % n
            HID, hn, addr = make_kasaya_id(True), "host_%i" % n, "tcp://%s:4000" % ip
            _hosts.append(HID)
            DB.host_register(HID, addr)

            # make workers
            for w in range(10):
                WID = make_kasaya_id(False) # worker id
                addr="tcp://%s:%i" % (ip, 5000+w)
                DB.worker_register(HID, WID, random.choice(svce), addr, n+((w+1)*100) )

        # check hosts
        hlst = list( DB.host_list() )
        self.assertItemsEqual(  [ x['id'] for x in hlst ] , _hosts )

        # check existance of workers in hosts
        for h in hlst:
            itms=0
            wids = []
            haddr = h['addr'].rsplit(":",1)[0]

            for w in DB.worker_list(h['id']):
                itms+=1
                waddr = w['addr'].rsplit(":",1)[0]
                self.assertEqual( waddr, haddr )
                # is this worker listed only one
                assert w['id'] not in wids, "Worker has been listed before!"
                wids.append(w['id'])

            # how many workers was on the list
            self.assertEqual(itms, 10)

        removed = []
        # check unregistering workers
        for h in DB.host_list():
            remove = False
            by_id = False
            for w in DB.worker_list(h['id']):
                # remove this worker
                if remove:
                    w['hid'] = h['id']
                    removed.append(w)
                    if by_id:
                        DB.worker_unregister(ID=w['id'])
                    else:
                        DB.worker_unregister(address=w['addr'])
                    by_id = not by_id
                remove = not remove

        # check removef workers
        for i in removed:
            w = DB.worker_get(i['id'])
            self.assertEqual(w,None, "Worker should be removed but still exists in database")

        # random worker choice
        for s in svce:
            for n in range(10):
                w = DB.choose_worker_for_service(s)
                wrkr = DB.worker_get(w['id'])
                self.assertEqual( wrkr['service'], s )

        # unknown service
        w = DB.choose_worker_for_service("foobar")
        self.assertEqual(w, None)

    def test_hostname_update(self):
        DB = NetworkStateDB()
        res = DB.host_register("Htest", "10.20.30.40:3456")
        # check whats registered
        hi = DB.host_info("Htest")
        self.assertEqual( hi['hostname'], None )
        # change hostname
        DB.host_set_hostname("Htest","host_234")
        hi = DB.host_info("Htest")
        self.assertEqual( hi['hostname'], "host_234" )
        # check host database
        hl = list( DB.host_list() )
        self.assertEqual(len(hl),1)
        self.assertEqual( hl[0]['hostname'], "host_234" )


if __name__ == '__main__':
    unittest.main()
