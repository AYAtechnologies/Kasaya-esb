#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os, random
# misc
from kasaya.conf import set_value, settings
from kasaya.core.events import add_event_handler, _purge_event_db
import gevent

from kasaya.workers.kasayad.pong import PingDB
from kasaya.core.lib import make_kasaya_id


class PingTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        set_value("KASAYAD_DB_BACKEND", "memory")

    def setUp(self):
        _purge_event_db()


    def test_ping_pong(self):
        res = {}

        def func_reg(ID, service, address, pid):
            res['reg'] = (ID, service, address, pid)
            #print ("reg", res)#ID, service, address, pid)

        def func_unreg(ID):
            res['unr'] = ID

        add_event_handler("worker-local-start", func_reg)
        add_event_handler("worker-local-stop", func_unreg)

        P = PingDB()
        w1 = make_kasaya_id()
        res['unr'] = None

        # simple ping will not add worker to db
        P.ping(w1)
        self.assertEqual( len(P._pingdb), 0 )

        P.ping_ex( w1, "foo", "tcp://127.0.0.1:1234", 567 )
        gevent.sleep(0)
        self.assertEqual( len(P._pingdb), 1 )
        # has been notification function triggered?
        self.assertItemsEqual( res['reg'], [w1, 'foo', 'tcp://127.0.0.1:1234', 567] )
        # dont call unregister
        self.assertEqual( res['unr'], None )

        # bad ping data
        res['reg'] = None
        P.ping_ex( w1, "bar", "tcp://127.0.0.1:1234", 567 )
        gevent.sleep(0)
        self.assertEqual( res['reg'], None )
        self.assertEqual( res['unr'], w1 )

        self.assertEqual( len(P._pingdb), 0 )


    def test_timeouts(self):
        res = {'unreg':[]}

        def func_reg(ID, service, address, pid):
            res['reg'] = (ID, service, address, pid)
        def func_unreg(ID):
            res['unreg'].append(ID)

        add_event_handler("worker-local-start", func_reg)
        add_event_handler("worker-local-stop", func_unreg)

        P = PingDB()
        w1 = (make_kasaya_id(), "foo", "tcp://127.0.0.1:1234", 789)
        w2 = (make_kasaya_id(), "baz", "tcp://127.0.0.1:1235", 790)

        P.ping_ex( *w1 )
        P.ping_ex( *w2 )
        gevent.sleep(0)

        # two pings
        self.assertEqual( len(P._pingdb), 2 )
        set_value('WORKER_HEARTBEAT',1)

        # check pings
        P.check_all()
        gevent.sleep(0)
        self.assertEqual( len(P._pingdb), 2 )
        self.assertEqual( len( res['unreg'] ), 0 )

        # timeout
        gevent.sleep(3)
        P.check_all()
        gevent.sleep(0)
        self.assertEqual( len(P._pingdb), 0 )
        self.assertEqual( len( res['unreg'] ), 2 )
        self.assertItemsEqual( [w1[0],w2[0]] , res['unreg'] )

        # ping of life
        res['reg'] = None
        res['unreg'] = []
        P.ping_ex( *w1 )
        P.ping_ex( *w2 )
        gevent.sleep(3)
        P.ping( w1[0] )
        P.check_all()
        gevent.sleep(0)
        self.assertItemsEqual( [w2[0]] , res['unreg'] )
        self.assertEqual( len( res['unreg'] ), 1 )
        #from pprint import pprint
        #pprint ( P._pingdb )


if __name__ == '__main__':
    unittest.main()
