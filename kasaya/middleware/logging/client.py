__author__ = 'wektor'
from servicebus.middleware.client import MiddlewareClient
from servicebus.client import sync, async, register_auth_processor, async_result, busctl

class Client(MiddlewareClient):

    def __init__(self, client):
        self.client = client

    def prepare_message(self, message):
        message["context"]["client"] = self.client
        sync.logging.log_client_call(message)
        return message

    def postprocess_message(self, message):
        sync.logging.log_client_received(message)
        return message