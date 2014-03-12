#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.lib import LOG, make_kasaya_id
from kasaya.core.client import Context


class WorkerBase(object):

    def __init__(self, is_host=False):
        self.ID = make_kasaya_id(host=is_host)




class TaskExecutor(object):

    def find_task(self, taskname):
        raise NotImplemented
        #raise KeyError

    def run_task(self, funcname, context, args, kwargs):
        # find task in worker db
        try:
            task = find_task(funcname)
        except KeyError:
            self._tasks_nonex += 1
            LOG.info("Unknown worker task called [%s]" % funcname)
            return exception_serialize_internal( 'Method %s not found' % funcname )

        # try to run function and catch exceptions
        try:
            LOG.debug("task %s, args %s, kwargs %s" % (funcname, repr(args), repr(kwargs)))
            func = task['func']
            tout = task['timeout']
            if tout is None:
                # call task without timeout
                result = func(*args, **kwargs)
            else:
                # call task with timeout
                with gevent.Timeout(tout, TaskTimeout):
                    result = func(*args, **kwargs)
            self._tasks_succes += 1
            task['res_succ'] += 1

            return {
                'message' : messages.RESULT,
                'result' : result
            }

        except TaskTimeout as e:
            # timeout exceeded
            self._tasks_error += 1
            task['res_tout'] += 1
            err = exception_serialize(e, internal=False)
            LOG.info("Task [%s] timeout (after %i s)." % (funcname, tout) )
            return err

        except Exception as e:
            # exception occured
            self._tasks_error += 1
            task['res_err'] += 1
            err = exception_serialize(e, internal=False)
            LOG.info("Task [%s] exception [%s]. Message: %s" % (funcname, err['name'], err['description']) )
            LOG.debug(err['traceback'])
            return err

        finally:
            # close django connection
            # if worker is using Django ORM we must close database connection manually,
            # or each task will leave one unclosed connection. This is done automatically.
            if task['close_djconn']:
                try:
                    _close_dj_connection()
                except Exception as e:
                    if e.__class__.__name__ == "ImproperlyConfigured":
                        # django connection is not required or diango orm is not used at all,
                        # because of that we replace _close_dj_connection function by empty lambda
                        global _close_dj_connection
                        _close_dj_connection = lambda:None
