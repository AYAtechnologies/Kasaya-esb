__author__ = 'wektor'
from base import BackendBase, TaskNotFound
import redis
import uuid
import datetime

class RedisBackend(BackendBase):
    def __init__(self):
        pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
        self.store = redis.Redis(connection_pool=pool)

    def add_task(self, task):
        task_id = str(uuid.uuid4())
        self.store.hmset(task_id, task)
        return task_id

    def del_task(self, task):
        return True

    def start_execution(self, task_id, daemon_id, worker):
        execution = {}
        execution["started"] = datetime.datetime.now()
        execution["daemon"] = daemon_id
        execution["finished"] = None
        execution["worker"] = worker
        self.store.hmset(task_id+"execution", execution)

    def get_result(self, task_id):
        try:
            task = self.store.hgetall(task_id)
            try:
                return task["result_type"], task["result"]
            except KeyError:
                return "in_progress", None
        except KeyError:
            raise TaskNotFound

    def set_result_success(self, task_id, result):
        self.store.hset(task_id+"execution", "finished", datetime.datetime.now())
        self.store.hset(task_id, "result_type", "ok")
        self.store.hset(task_id, "result", result)

    def set_result_fail(self, task_id, error_code, error):
        self.store.hset(task_id+"execution", "finished", datetime.datetime.now())
        self.store.hset(task_id, "result_type", error_code)
        self.store.hset(task_id, "result", repr(error))
