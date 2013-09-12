#coding: utf-8
__author__ = 'wektor'
from kasaya.conf import settings
from kasaya.core.backend import get_backend_class
import uuid
import datetime

Backend = get_backend_class(settings.ASYNC_DAEMON_DB_BACKEND)

class AsyncBackend(object):
    TASK_LINK = "t:" # link to current state of the task
    TASK_WAITING = "w:"
    TASK_IN_PROGRESS = "p:"
    TASK_COMPLETE = "c:"
    DEMON_LIST = "demons:"
    DEMON_TASK_LIST = "dl:" # open tasks for demon

    def __init__(self, async_id, async_connection="1111"):
        self.backend = Backend()
        self.async_id = async_id
        self.async_connection = async_connection

    def sanitize_task(self, task):
        print "WRONG TASK: ", task

    def get_task(self, task_id):
        t_id = self.backend.get(self.TASK_LINK + task_id)
        return self.backend.get(t_id)

    def add_task(self, task):
        task_id = str(uuid.uuid4())
        self.backend.set(self.TASK_LINK + task_id, self.TASK_WAITING + task_id)
        task["daemon_id"] = self.async_id
        task["daemon_ip"] = self.async_connection
        task["status"] = self.TASK_WAITING
        self.backend.set(self.TASK_WAITING + task_id, task)
        loose_tasks = []
        try:
            demon_id = self.backend.get(self.DEMON_LIST+self.async_connection)
            if demon_id != self.async_id:
                #a deamon is hanging with the same ip/port - it is probably dead
                loose_tasks = self.backend.get(self.DEMON_TASK_LIST + demon_id).keys()
        finally:
            # set self as demon for ip/port
            self.backend.set(self.DEMON_LIST+self.async_connection, self.async_id)
         # setadd
        self.setadd(self.DEMON_TASK_LIST + self.async_id, task_id)
        return task_id, loose_tasks


    def setadd(self, key, set_item):
        task_set = self.backend.get(key)
        try:
            task_set[set_item] = 1
        except:
            task_set = {set_item:1}
        self.backend.set(key, task_set)

    def setrem(self, key, set_item):
        task_set = self.backend.get(key)
        try:
            del task_set[set_item]
        except KeyError:
            pass
        self.backend.set(key, task_set)


    def del_task(self, task):
        return True

    def start_execution(self, task_id, worker):
        execution = {}
        execution["started"] = str(datetime.datetime.now())
        execution["finished"] = None
        execution["worker"] = worker
        task = self.backend.get(self.TASK_WAITING + task_id)
        task["status"] = self.TASK_IN_PROGRESS
        task.update(execution)
        self.backend.set(self.TASK_LINK + task_id, self.TASK_IN_PROGRESS + task_id)
        self.backend.set(self.TASK_IN_PROGRESS + task_id, task)
        self.backend.delete(self.TASK_WAITING + task_id)

    def get_result(self, task_id):
        try:
            task = self.backend.get(self.TASK_IN_PROGRESS +task_id)
            try:
                task = self.backend.get(self.TASK_COMPLETE +task_id)
                return task["result_type"], task["result"]
            except:
                return "in_progress", None
        except KeyError:
            raise TaskNotFound

    def finish_task(self, task_id, result_type, result):
        task = self.backend.get(self.TASK_IN_PROGRESS + task_id)
        task["finished"] = str(datetime.datetime.now())
        task["status"] = self.TASK_COMPLETE
        task["result_type"] = result_type
        task["result"] = result
        self.backend.set(self.TASK_LINK + task_id, self.TASK_COMPLETE + task_id)
        self.setrem(self.DEMON_TASK_LIST + self.async_id, task_id)
        self.backend.set(self.TASK_COMPLETE + task_id, task)
        self.backend.delete(self.TASK_IN_PROGRESS + task_id)

    def set_result_success(self, task_id, result):
        self.finish_task(task_id, "ok", result)

    def set_result_fail(self, task_id, error_code, error):
        self.finish_task(task_id, error_code, repr(error))


class TaskNotFound(Exception):
    pass