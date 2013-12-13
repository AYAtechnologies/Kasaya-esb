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
                error INTEGER,               /* error counter */
                delay INTEGER,               /* next execution time */
                args BLOB,                   /* args,kwargs */
                context BLOB,                /* context */
                result BLOB                  /* stored result */
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
        query = "INSERT INTO jobs (task, time_crt, args, context, status, error, delay) VALUES (?,?,?,?,0,0,0)"
        res = self.cur.execute( query, (taskname, time, SQ.Binary(args), SQ.Binary(context) ) )
        rowid = res.lastrowid
        self.__db.commit()
        return "%s-%X" % (self.dbid, rowid)


    def task_get_next_id(self):
        """
        Chooses one task waiting for execution.
        This function should use atomic database features to make sure that only
        one async daemon connected to this database choose task even if ask for
        job in same time.

        statusy: 0 - waiting, 1 - selected to process, 2 - processing, 3 - finished
        """
        now = time.time()
        # check if there is any missing task selected for processing
        query = "SELECT taskid FROM jobs WHERE status=1 AND asyncid=? LIMIT 1"
        self.cur.execute( query, (self.ID,) )
        res = self.cur.fetchone()

        if res is None:
            # nothing already selected, then select one
            query = "UPDATE jobs SET asyncid=?, status=1, time_act=? WHERE status=0 AND delay<=? LIMIT 1"
            res = self.cur.execute( query, (self.ID,time.time(), now ) )
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
        return rowid


    def task_get_to_process(self, taskid):
        # set task status to 2 (processing)
        query = "UPDATE jobs SET status=2, time_act=? WHERE taskid=? AND status=1"
        self.cur.execute( query, (taskid,time.time()) )
        self.__db.commit()
        # get all required task data
        query = "SELECT asyncid,time_crt,task,args,context FROM jobs WHERE taskid=?"#" AND status=2"
        self.cur.execute( query, (taskid,) )
        res = self.cur.fetchone()
        if res is None:
            return None
        return {
            #'id'       : taskid,
            'time_crt' : res[1],
            'task'     : res[2],
            'args'     : str(res[3]),
            'context'  : str(res[4]),
       }


    def task_error_and_delay(self, taskid, delay):
        """
        Increase task error counter and set delay time before next try
        """
        now = time.time()
        delay = now + delay
        query = "UPDATE jobs SET status=0, time_act=?, error=error+1, delay=? WHERE taskid=?"
        self.cur.execute( query, (now, delay, taskid) )
        self.__db.commit()


    def task_finished(self, taskid, result, error):
        """
        Set result of task
        """
        pass
