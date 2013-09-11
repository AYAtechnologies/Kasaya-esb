__author__ = 'wektor'
from servicebus.client import sync, async, async_result, control
from servicebus.middleware.worker import MiddlewareBaseWorker

class WorkerMiddleware(MiddlewareBaseWorker):

    def __init__(self, worker):
        self.worker = worker

    def prepare_message(self, message):
        try:
            message["context"]["worker"] = str(self.worker)
        except:
            message["context"] = {"worker": str(self.worker)}
        sync.logging.log_worker_received(message)
        return message

    def postprocess_message(self, message):
        sync.logging.log_worker_respond(message)
        return message