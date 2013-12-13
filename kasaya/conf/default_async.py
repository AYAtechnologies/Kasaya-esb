# coding: utf-8
#
# Async daemon default settings
#
from __future__ import division, absolute_import, print_function, unicode_literals


# async daemon internal database backend
ASYNC_DB_BACKEND = "sqlite"

# if sqlite database is used, where will be stored database file
ASYNC_SQLITE_DB_PATH = "/tmp/kasaya_async_db.sqlite"

# delay in seconds before task sended to unexisting worker will be retried
ASYNC_ERROR_TASK_DELAY = 30
