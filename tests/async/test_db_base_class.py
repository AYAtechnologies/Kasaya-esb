#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, unicode_literals
#from kasaya.conf import set_value #, settings
import unittest #, os, random
# misc
from kasaya.workers.asyncd.db.base import DatabaseBase


class dbtest(DatabaseBase):

    def _config_value_get(self, key):
        global kvdb
        return kvdb.get(key, None)
    def _config_value_set(self, key, val):
        global kvdb
        kvdb[key]=val
    def _config_del(self, key):
        global kvdb
        try:
            del kvdb[key]
        except:
            pass


class AsyncDBBaseClassTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global kvdb
        kvdb = {}


    def test_db_baseclass(self):
        """
        Base DB class and database id setting check
        """
        db1 = dbtest("A")
        id1 = db1.CONF['databaseid']
        db2 = dbtest("B")
        id2 = db2.CONF['databaseid']
        # different instances keep same db ID
        self.assertEqual(id1, id2)
        # deleting keys
        del db1.CONF['databaseid']
        self.assertEqual(db2.CONF['databaseid'], None)




if __name__ == '__main__':
    unittest.main()
