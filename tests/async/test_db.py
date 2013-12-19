#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, unicode_literals
from kasaya.conf import set_value #, settings
import unittest #, os, random

from kasaya.workers.asyncd.db.sqlite import SQLiteDatabase


class AsyncDatabaseTest(object):

    def test_db_config_store(self):
        # two database instances connected to same databse
        DB1 = self.DBBackend("AAAA")
        DB2 = self.DBBackend("BBBB")
        self.assertEqual( DB1.CONF['databaseid'],  DB1.CONF['databaseid'] )



class AsyncDatabaseTest(unittest.TestCase, AsyncDatabaseTest):

    @classmethod
    def setUpClass(cls):
        fname = "/tmp/kasaya_async_test_db.sqlite"
        import os
        try:
            os.unlink(fname)
        except OSError:
            pass
        set_value("ASYNC_SQLITE_DB_PATH", fname)
        cls.DBBackend = SQLiteDatabase


if __name__ == '__main__':
    unittest.main()
