__author__ = 'wektor'

import uuid
import datetime
from base import BackendBase, TaskNotFound

class DictBackend(BackendBase):
    store = {}
    def add_task(self, task):
        task_id = str(uuid.uuid4())
        self.store[task_id] = {}
        self.store[task_id]["task"] = task
        return task_id

    def start_execution(self, task_id, daemon_id, worker):
        execution = {}
        execution["started"] = datetime.datetime.now()
        execution["daemon"] = daemon_id
        execution["finished"] = None
        execution["worker"] = worker
        self.store[task_id]["execution"] = execution


    def get_result(self, task_id):
        try:
            task = self.store[task_id]
            try:
                return task["result_type"], task["result"]
            except KeyError:
                return "in_progress", None
        except KeyError:
            raise TaskNotFound

    def set_result_success(self, task_id, result):
        self.store[task_id]["execution"]["finished"] = datetime.datetime.now()
        self.store[task_id]["result_type"] = "ok"
        self.store[task_id]["result"] = result

    def set_result_fail(self, task_id, error_code, error):
        self.store[task_id]["execution"]["finished"] = datetime.datetime.now()
        self.store[task_id]["result_type"] = error_code
        self.store[task_id]["result"] = str(error)