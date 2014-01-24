#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.core import exceptions
#from kasaya.core.protocol import messages
#from kasaya.conf import settings
#from .generic_proxy import GenericProxy

__all__ = ("AsyncResult",)


class AsyncResult(object):

    def __init__(self, taskid):
        print ("             taskid",taskid)
        if not type(taskid) in (str, unicode):
            raise Exception("Not valid task id")
        self.__id = taskid

    def __get_result(self):
        global sync
        try:
            res = sync.async.get_task_result(self.__id)
        except NameError:
            from kasaya import sync
            res = sync.async.get_task_result(self.__id)
        #print (res)

    # simple flags
    def is_ready(self):
        """
        Is task processed and finished?
        """
        pass

    def is_error(self):
        """
        Is task finished with error
        true - task is processed, but produced exception
        false - task is unprocessed or finished succesfully
        """
        pass

    def is_waiting(self):
        """
        Task is waiting for process
        true - waiting
        false - processed
        """
        pass

    def error_counter(self):
        """
        Retry counter for this task. This counter return internal kasaya error counter, not number of erros in worker
        """
        pass

    # numerical status
    def status(self):
        """
        Status code of task
        0 - waiting for process
        1 - processing
        2 - finished succesfully
        3 - finished with error
        """
        pass

    @property
    def result(self):
        """
        Return result of task, or raises exception thrown by worker.
        If task is not processed, it will raise NotProcessedTask exception
        """
        self.__get_result()


    def __unicode__(self):
        return unicode( self.__id )
    def __str__(self):
        return str( self.__id )

    def __repr__(self):
        return "<KasayaAsyncResult:%s>" % self

