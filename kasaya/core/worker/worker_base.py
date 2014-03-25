#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.lib import LOG, make_kasaya_id
from kasaya.core.lib import django_integration as DJI
from kasaya.core.client import Context
from kasaya.core.protocol import messages
from kasaya.core.protocol.comm import send_and_receive, exception_serialize_internal, exception_serialize, ConnectionClosed
import gevent
import weakref


class WorkerBase(object):

    def __init__(self, is_host=False):
        self.ID = make_kasaya_id(host=is_host)
        self.stats = {}


    def stat_increment(self, name, increment=1):
        """
        Increment counter with given name by value increment (default=1)
        """
        if name in self.stats:
            self.stats[name]+=increment
        else:
            self.stats[name]=increment



class TaskExecutor(object):


    def _find_task(self, taskname):
        raise NotImplemented
        #raise KeyError


    def context_prepare(self, ctx):
        """
        Called before executing task
        """
        LOG.debug ("prepare " +  repr(ctx) )
        pass

    def context_postprocess(self, ctx):
        """
        Called after execution of task if task is completed properly.
        """
        pass

    def context_postprocess_exception(self, ctx, exception):
        """
        Called after task execution if task raised exception.
        """
        pass



    def handle_task_request(self, msgdata):
        """
        Prepare data for processing in task. Extracts parameters from message and check context.
        Valid request requires in message such fields: method, context, args, kwargs
        """
        if msgdata['service']!=self.servicename:
            # this task is adressed for other service!
            raise exceptions.ServiceBusException("Wrong service called %s" % str(service) )

        # worker is not online
        if self.status!=2:
            raise exceptions.ServiceBusException("Worker is currently offline")

        # find task in worker db
        try:
            task = self._find_task( msgdata['method'] )
        except KeyError:
            self.stat_increment('tasks_nonex')
            LOG.info("Unknown worker task called [%s]" % task['name'])
            return exception_serialize_internal( 'Method %s not found' % task['name'] )

        # logging
        LOG.debug("task %s, args %s, kwargs %s" % (msgdata['method'], repr(msgdata['args']), repr(msgdata['kwargs'])))

        # context instance
        ctx = msgdata['context']
        if not isinstance(ctx, Context):
            ctx = Context(msgdata['context'])

        # execute!
        result = self.run_task(
            task = task,
            context = ctx,
            args = msgdata['args'],
            kwargs = msgdata['kwargs']
        )
        return result



    def run_task(self, task, context, args, kwargs):
        """
        Execute task and return result or raised exception.
        """
        # create greenlet
        grn = gevent.Greenlet( task['func'], *args, **kwargs )
        grn.context = context
        # run it!
        grn.start()
        try:
            # process context
            self.context_prepare(context)
            if task['timeout'] is None:
                grn.join()
            else:
                # detect timeout!
                try:
                    with gevent.Timeout(task['timeout'], TaskTimeout):
                        grn.join()
                except TaskTimeout as e:
                    LOG.info("Task [%s] timeout (after %i s)." % (task['name'], task['timeout']) )
                    self.stat_increment('tasks_error')
                    task['res_tout'] += 1   # increment task's timeout exception counter
                    err = exception_serialize(e, internal=False)
                    self.context_postprocess_exception(context,e)
                    return err

        except Exception as e:
            err = exception_serialize(e, internal=False)

        finally:
            # cleanup after task execution
            if task['close_djconn']:
                DJI.close_django_conn()

        if grn.successful():
            # task processed succesfully
            self.stat_increment('tasks_succes')
            task['res_succ'] += 1
            self.context_postprocess(context)
            return {
                'message' : messages.RESULT,
                'result' : grn.value
            }
        else:
            # task raised error
            self.stat_increment('tasks_error')
            task['res_err'] += 1
            err = exception_serialize(grn.exception, internal=False)
            self.context_postprocess_exception(context, grn.exception)
            LOG.info("Task [%s] exception [%s]. Message: %s" % (task['name'], err['name'], err['description']) )
            LOG.debug(err['traceback'])
            return err
