#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core.lib import LOG, make_kasaya_id
from kasaya.core.lib import django_integration as DJI
from kasaya.core.client import Context
from kasaya.core.protocol import messages
from kasaya.core.protocol.comm import send_and_receive, ConnectionClosed
import weakref
import traceback


class WorkerBase(object):

    def __init__(self, servicename, is_host=False):
        global gevent
        import gevent
        self.ID = make_kasaya_id(host=is_host)
        self.servicename = servicename
        self.stats = {}

        if LOG.level<=10:
            from kasaya.conf import settings
            for k,v in settings.items():
                LOG.debug("settings %s = %r" % (k,v))


    def verbose_name(self):
        """
        Return verbose name of worker for exceptions
        """
        return "[%s] %s" % (self.ID, self.servicename)

    def stat_increment(self, name, increment=1):
        """
        Increment counter with given name by value increment (default=1)
        """
        if name in self.stats:
            self.stats[name]+=increment
        else:
            self.stats[name]=increment



def exception_catcher(func, *args, **kwargs):
    """
    This function catches exceptions and tracebacks inside greenlets.
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        tb = traceback.format_exc()
    e.traceback = tb
    raise e


class TaskExecutor(object):


    def _find_task(self, taskname):
        raise NotImplemented
        #raise KeyError

    def context_prepare(self, ctx):
        """
        Called before executing task
        """
        pass

    def process_exception_message(self, task, context, msg):
        """
        Process result message of task if thrown exception.
        This function can change message in place (msg prameter).
        """
        name = self.verbose_name()
        name += ".%s" % task['name']
        if not msg['remote']:
            name += " (exception thrown)"
        msg['request_path'].insert(0, name )

    def process_result_message(self, task, context, msg):
        """
        Process result message of task.
        This function can change message in place (msg parameter).
        """
        pass


    # task handling


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
            return messages.exception2message( 'Method %s not found' % task['name'], True )

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
        grn = gevent.Greenlet( exception_catcher, task['func'], *args, **kwargs )
        #grn = gevent.Greenlet( task['func'], *args, **kwargs )
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
                    #LOG.info("Task [%s] timeout (after %i s)." % (task['name'], task['timeout']) )
                    self.stat_increment('tasks_error')
                    task['res_tout'] += 1  # increment task's timeout exception counter
                    msg = messages.exception2message(e)
                    self.process_exception_message(task, context, msg)
                    return msg

        except Exception as e:
            # this exception can be raised only by context_prepare,
            # because tasks exceptions will be stored in greenlet object
            self.stat_increment('tasks_error')
            task['res_err'] += 1
            msg = messages.exception2message(e)
            self.process_exception_message(task, context, msg)
            return msg

        finally:
            # cleanup after task execution
            if task['close_djconn']:
                DJI.close_django_conn()

        if grn.successful():
            # task processed succesfully
            self.stat_increment('tasks_succes')
            task['res_succ'] += 1
            msg = messages.result2message(grn.value)
            self.process_result_message(task, context, msg)
            return msg
        else:
            # task raised error
            self.stat_increment('tasks_error')
            task['res_err'] += 1
            msg = messages.exception2message(grn.exception)
            self.process_exception_message(task, context, msg)
            return msg
