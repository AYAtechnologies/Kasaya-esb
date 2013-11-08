#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os, random
# misc
from kasaya.conf import set_value, settings

from kasaya.workers.kasayad.db.netstatedb import NetworkStateDB
from kasaya.core.lib import make_kasaya_id


class EncryptionTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        set_value("KASAYAD_DB_BACKEND", "memory")

    def test_host_up_and_down(self):
        DB = NetworkStateDB()
        ID, hn, ip = make_kasaya_id(True), "host_1", "192.168.1.2"

        # adding host
        res = DB.host_register(ID, hn, ip)
        self.assertEqual(res, True)
        res = DB.host_register(ID, hn, ip)
        self.assertEqual(res, False)

        # listing hosts
        lst = list( DB.host_list() )
        self.assertEqual( len(lst), 1 )
        self.assertEqual( lst[0]['id'], ID )
        self.assertEqual( lst[0]['hostname'], hn )
        self.assertEqual( lst[0]['ip'], ip )

        # get host by ID
        h = DB.host_addr_by_id(ID)
        self.assertEqual( h, ip )

        # unregister
        DB.host_unregister(ID)

        # check unregistering result
        lst = list( DB.host_list() )
        self.assertEqual( len(lst), 0 )
        h = DB.host_addr_by_id(ID)
        self.assertEqual( h, None )

        # register 10 hosts
        for n in range(10,20):
            ID, hn, ip = make_kasaya_id(True), "host_%i" % n, "192.168.1.%i" % n
            res = DB.host_register(ID, hn, ip)
            self.assertEqual(res, True)
            res = DB.host_register(ID, hn, ip)
            self.assertEqual(res, False)

        # is there 10 hosts on list?
        lst = list( DB.host_list() )
        self.assertEqual( len(lst), 10 )

        # check host query
        for n in range(10,20):
            DB.host_addr_by_id(ID)
        DB.close()


    def test_host_with_workers(self):
        _hosts = []

        DB = NetworkStateDB()
        svce = ("mail","sms","bubbles","spam")
        # create hosts
        for n in range(6):
            HID, hn, ip = make_kasaya_id(True), "host_%i" % n, "192.168.1.%i" % n
            _hosts.append(HID)
            DB.host_register(HID, hn, ip)

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
            for w in DB.worker_list(h['id']):
                itms+=1
                ip = w['addr'].split("//")[1]
                ip = ip.split(":")[0]
                self.assertEqual( h['ip'], ip )
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




if __name__ == '__main__':
    unittest.main()
