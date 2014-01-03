#coding: utf-8
from __future__ import unicode_literals, division, absolute_import#, print_function
from kasaya.conf import settings
import sqlite3 as SQ
import time
from .base import DatabaseBase


class SQLiteDatabase(DatabaseBase):

    def __init__(self, workerid):
        self.__db = SQ.connect(settings.ASYNC_SQLITE_DB_PATH)
        self.cur = self.__db.cursor()

        # creating tables
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                taskid INTEGER PRIMARY KEY,  /* task unique ID */
                asyncid TEXT,                /* async daemon ID */
                status INTEGER,              /* 0 - waiting, 1 - selected to process, 2 - processing, 3 - finished */
                time_crt INTEGER,            /* task creation time */
                time_act INTEGER,            /* last activity time */
                ignore_result BOOL,          /* ignore result of task */
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
        self.__db.commit()
        # synchronous
        self.cur.execute("""PRAGMA synchronous = %s""" %
            ( settings.ASYNC_SQLITE_DB_SYNCHRONOUS.strip().upper(), )
        )
        super(SQLiteDatabase, self).__init__(workerid)


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

    def task_add(self, taskname, time, args, context, ign_result):
        """
        Adds task to database for execution
        """
        query = "INSERT INTO jobs (task, time_crt, args, context, ignore_result, status, error, delay) VALUES (?,?,?,?,?,0,0,0)"
        res = self.cur.execute( query, (taskname, time, SQ.Binary(args), SQ.Binary(context), ign_result ) )
        rowid = res.lastrowid
        self.__db.commit()
        return "%s-%X" % (self.dbid, rowid)


    def task_choose_for_process(self):
        """
        Chooses one task waiting for execution.
        This function should use atomic database features to make sure that only
        one async daemon connected to this database choose task even if ask for
        job in same time.

        statusy: 0 - waiting, 1 - selected to process, 2 - processing, 3 - finished, 4 - error permanent
        """
        now = time.time()

        # select free task
        query = "UPDATE jobs SET asyncid=?, status=1, time_act=? WHERE status=0 AND delay<=? LIMIT 1"
        res = self.cur.execute( query, (self.ID,time.time(), now ) )
        try:
            self.__db.commit()
        except SQ.OperationalError:
            # database is locked, can't select task for processing
            return None
        # nothing is waiting in task queue
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


    def task_start_process(self, taskid):
        """
        Get next free task for processing. This method guarantee that only one
        async daemon take same task by setting status of task to 1 and then selecting
        this task in next step.

        TODO: join function into single one with  'task_choose_for_process'
        """
        # set task status to 2 (processing)
        query = "UPDATE jobs SET status=2, time_act=? WHERE taskid=? AND status=1"
        self.cur.execute( query, (time.time(), taskid) )
        try:
            self.__db.commit()
        except SQ.OperationalError:
            return None
        # get all required task data
        query = "SELECT asyncid,time_crt,task,args,context FROM jobs WHERE taskid=? AND status=2"
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


    # tasks recovery


    def recover_unprocessed_tasks(self, time_limit):
        """
        Find all tasks unassigned to any asyncid with status=1 (selected for process) and restore
        their status to 0 (waiting for process).
        """
        badtime = time.time() - time_limit
        query = "UPDATE jobs SET asyncid=?, status=0, time_act=? WHERE asyncid is Null AND status=1 AND time_act<?"
        c = self.cur.execute( query, (None, time.time(), badtime) )
        try:
            self.__db.commit()
        except SQ.OperationalError:
            return 0
        return c.rowcount


    def recover_unfinished_tasks(self):
        """
        Find tasks with processing started, but with never unreceived result.
        This can happen, when async daemon died after sending task to worker,
        but before receiving response.
        Return number of affected rows.
        """
        query = "UPDATE jobs SET asyncid=?, status=0, error=error+1 WHERE status=2 AND asyncid is Null"
        c = self.cur.execute( query, (self.ID,) )
        try:
            self.__db.commit()
        except SQ.OperationalError:
            return 0
        return c.rowcount


    def async_list(self):
        """
        Return list of all async workers registered in database.
        Exclude self and tasks finished or permanently broken.
        """
        query = "SELECT DISTINCT asyncid FROM jobs WHERE asyncid!=? AND status<3"
        self.cur.execute( query, (self.ID,) )
        lst = self.cur.fetchall()
        for row in lst:
            yield row[0]


    def unlock_lost_tasks(self, asyncid):
        """
        Reassign all tasks of specified asyncd worker to None.
        Don't change finished or permanently broken tasks.
        This allow to process tasks again by other async daemon.
        """
        res = self.cur.execute( "UPDATE jobs SET asyncid=? WHERE asyncid=? AND status<3", (None, asyncid) )
        self.__db.commit()
        return res.rowcount


    # task processing


    def task_finished_ok(self, taskid, result):
        """
        Set result of task as successfull
        """
        now = time.time()
        query = "UPDATE jobs SET status=3, time_act=?, delay=0, result=? WHERE taskid=?"
        self.cur.execute( query, (now, SQ.Binary(result), taskid) )
        self.__db.commit()


    def task_error_and_delay(self, taskid, delay, result=None):
        """
        Increase task error counter and set delay time before next try.
        This means that error is recoverable and task can be repeated.
        """
        now = time.time()
        delay = now + delay
        query = "UPDATE jobs SET status=0, time_act=?, error=error+1, delay=?, result=? WHERE taskid=?"
        if not result is None:
            result = SQ.Binary(result)
        self.cur.execute( query, (now, delay, result, taskid) )
        self.__db.commit()



    def task_fail_permanently(self, taskid, result=None):
        """
        Sets task as permanently failed. Task will be never repeated.
        """
        now = time.time()
        query = "UPDATE jobs SET status=4, time_act=?, delay=0, result=? WHERE taskid=?"
        if not result is None:
            result = SQ.Binary(result)
        self.cur.execute( query, (now, result, taskid) )
        self.__db.commit()



    def task_store_context(self, taskid, context):
        query = "UPDATE jobs SET context=? WHERE taskid=?"
        if not context is None:
            context = SQ.Binary(context)
        self.cur.execute( query, (context, taskid) )
        self.__db.commit()


