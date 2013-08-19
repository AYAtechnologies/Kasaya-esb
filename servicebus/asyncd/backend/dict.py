__author__ = 'wektor'

import uuid
from base import BackendBase, TaskNotFound

class DictBackend(BackendBase):
    store = {}

    def add_task(self, task):
        task_id = str(uuid.uuid4())
        self.store[task_id] = {}
        self.store[task_id]["task"] = task
        self.store[task_id]["worker"] = task
        return task_id

    def get_result(self, task_id):
        try:
            task = self.store[task_id]
            try:
                return task["result"]
            except KeyError:
                return None
        except KeyError:
            raise TaskNotFound