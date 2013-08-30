__author__ = 'wektor'
from servicebus.client import sync, async, register_auth_processor, async_result, busctl

class WorkerMiddleware(object):

    def __init__(self, worker):
        pass

    def prepare_message(self, message):
        print "checking authorization", message
        if sync.auth.is_authorized_for_service(message["service"], message["context"]):
            return message
        else:
            raise Exception("Not Authorized")


    def postprocess_message(self, message):
        return message