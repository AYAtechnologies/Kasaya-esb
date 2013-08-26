__author__ = 'wektor'
from base import BackendBase, TaskNotFound
import redis
import uuid
import datetime

class RedisBackend(BackendBase):
    TASK_LINK = "t:" # link to current state of the task
    TASK_WAITING = "w:"
    TASK_IN_PROGRESS = "p:"
    TASK_COMPLETE = "c:"
    DEMON_LIST = "demons:"
    DEMON_TASK_LIST = "dl:" # open tasks for demon

    def __init__(self, async_id, async_connection):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.store = redis.Redis(connection_pool=pool)
        self.async_id = async_id
        self.async_connection = async_connection

    def sanitize_task(self, task):
        print "WRONG TASK: ", task

    def get_task(self, task_id):
        t_id = self.store.get(self.TASK_LINK + task_id)
        return self.store.hgetall(t_id)

    def add_task(self, task):
        task_id = str(uuid.uuid4())
        self.store.set(self.TASK_LINK + task_id, self.TASK_WAITING + task_id)
        task["daemon_id"] = self.async_id
        task["daemon_ip"] = self.async_connection
        task["status"] = self.TASK_WAITING
        self.store.hmset(self.TASK_WAITING + task_id, task)
        loose_tasks = []
        try:
            demon_id = self.store.hget(self.DEMON_LIST, self.async_connection)
            if demon_id != self.async_id:
                #a deamon is hanging with the same ip/port - it is probably dead
                loose_tasks = self.store.smembers(self.DEMON_TASK_LIST + demon_id)
        finally:
            # set self as demon for ip/port
            self.store.hset(self.DEMON_LIST, self.async_connection, self.async_id)
        self.store.sadd(self.DEMON_TASK_LIST + self.async_id, task_id)
        return task_id, loose_tasks

    def del_task(self, task):
        return True

    def start_execution(self, task_id, worker):
        execution = {}
        execution["started"] = datetime.datetime.now()
        execution["finished"] = None
        execution["worker"] = worker
        task = self.store.hgetall(self.TASK_WAITING + task_id)
        task["status"] = self.TASK_IN_PROGRESS
        task.update(execution)
        self.store.set(self.TASK_LINK + task_id, self.TASK_IN_PROGRESS + task_id)
        self.store.hmset(self.TASK_IN_PROGRESS + task_id, task)
        self.store.delete(self.TASK_WAITING + task_id)

    def get_result(self, task_id):
        try:
            task = self.store.hgetall(self.TASK_IN_PROGRESS +task_id)
            try:
                task = self.store.hgetall(self.TASK_COMPLETE +task_id)
                return task["result_type"], task["result"]
            except KeyError:
                return "in_progress", None
        except KeyError:
            raise TaskNotFound

    def finish_task(self, task_id, result_type, result):
        task = self.store.hgetall(self.TASK_IN_PROGRESS + task_id)
        task["finished"] = datetime.datetime.now()
        task["status"] = self.TASK_COMPLETE
        task["result_type"] = result_type
        task["result"] = result
        self.store.set(self.TASK_LINK + task_id, self.TASK_COMPLETE + task_id)
        self.store.srem(self.DEMON_TASK_LIST + self.async_id, task_id)
        self.store.hmset(self.TASK_COMPLETE + task_id, task)
        self.store.delete(self.TASK_IN_PROGRESS + task_id)

    def set_result_success(self, task_id, result):
        self.finish_task(task_id, "ok", result)

    def set_result_fail(self, task_id, error_code, error):
        self.finish_task(task_id, error_code, repr(error))
