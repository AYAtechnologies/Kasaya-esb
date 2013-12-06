#coding: utf-8
from __future__ import unicode_literals, division, absolute_import#, print_function
from kasaya.conf import settings
import sqlite3 as SQ
import time



class kvstore(object):
    def __init__(self, db):
        self.db = db

    def __getitem__(self, k):
        return self.db._config_value_get(k)

    def __setitem__(self, k,v):
        self.db._config_value_set(k,v)

    def __delitem__(self, k):
        self.db._config_del(k)




class SQLiteDatabase(object):

    def __init__(self, workerid):
        self.__db = SQ.connect(settings.ASYNC_SQLITE_DB_PATH)
        self.cur = self.__db.cursor()
        self.ID = workerid

        # creating tables
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                taskid INTEGER PRIMARY KEY,  /* task unique ID */
                asyncid TEXT,                /* async daemon ID */
                status INTEGER,              /* 0 - waiting, 1 - selected to process, 2 - processing, 3 - finished */
                time_crt INTEGER,            /* task creation time */
                time_act INTEGER,            /* last activity time */
                workerid TEXT,               /* worker id which received task */
                task TEXT,                   /* task name */
                args BLOB,                   /* args,kwargs */
                context BLOB,                /* context */
                result BLOB,                 /* stored result */
                error INTEGER               /* error 0 - ok, 1 - error */
            )""")
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS conf (
                key TEXT PRIMARY KEY,
                val TEXT
            )""")

        # dictionary interface for config
        self.CONF = kvstore(self)

        # database ID
        self.dbid = self.CONF['databaseid']
        if self.dbid is None:
            import os, base64
            code = base64.b32encode( os.urandom(5) )
            self.dbid = code.decode("ascii").rstrip("=")
            self.CONF['databaseid'] = self.dbid


    def close(self):
        self.__db.close()

    # config inteface

    def _config_value_get(self, key):
        self.cur.execute( "SELECT val FROM conf WHERE key=?", (key,) )
        result = self.cur.fetchone()
        if result is None:
            return None
        return result[0]

    def _config_value_set(self, key, val):
        #self.cur.execute( "DELETE FROM conf WHERE key=?", (key,) )
        self.cur.execute( "INSERT OR IGNORE INTO conf (key,val) VALUES (?,?)", (key,val) )
        self.cur.execute( "UPDATE conf SET val=? WHERE key=?", (val, key) )
        self.__db.commit()

    def _config_del(self, key):
        self.cur.execute("DELETE FROM conf WHERE key=?", (key,) )
        self.__db.commit()

    # task database

    def task_add(self, taskname, time, args, context):
        """
        Adds task to database for execution
        """
        query = "INSERT INTO jobs (task, time_crt, args, context, status) VALUES (?,?,?,?,?)"
        res = self.cur.execute( query, (taskname, time, SQ.Binary(args), SQ.Binary(context), 0 ) )
        rowid = res.lastrowid
        self.__db.commit()
        return "%s-%X" % (self.dbid, rowid)

    def task_get_next(self):
        """
        Chooses one task waiting for execution.
        This function should use atomic database features to make sure that only
        one async daemon connected to this database choose task even if ask for
        job in same time.
        """
        # check if there is any missing task selected for processing
        query = "SELECT taskid FROM jobs WHERE status=1 AND asyncid=? LIMIT 1"
        self.cur.execute( query, (self.ID,) )
        res = self.cur.fetchone()

        if res is None:
            # nothing already selected, then select one
            query = "UPDATE jobs SET asyncid=?, status=1, time_act=? WHERE status=0 LIMIT 1"
            res = self.cur.execute( query, (self.ID,time.time() ) )
            self.__db.commit()
            # nothing is waiting tasks in queue
            if res.rowcount==0:
                return None

            query = "SELECT taskid FROM jobs WHERE status=1 AND asyncid=? LIMIT 1"
            self.cur.execute( query, (self.ID,) )
            res = self.cur.fetchone()

        # nothing found in this place is strange...
        if res is None:
            return None

        rowid = res[0]
        print rowid


    def task_check(self, taskid):
        pass

    def task_get(self, taskid):
        pass


