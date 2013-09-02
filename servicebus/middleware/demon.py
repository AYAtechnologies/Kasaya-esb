__author__ = 'wektor'
from servicebus.worker import Daemon
from servicebus.middleware.core import MiddlewareCore

class MiddlewareDemon(Daemon):
    pass
    # def __init__(self, name):
    #     WorkerDaemon.__init__(self, name)
    #     MiddlewareCore.__init__(self)
    #
    # def run(self):
    #     self.setup_middleware()
    #     self._run()
    #
    # def handle_sync_call(self, msgdata):
    #     name = msgdata['service']
    #     msgdata = self.prepare_message(msgdata)
    #     result = self.run_task(
    #         funcname = msgdata['method'],
    #         args = msgdata['args'],
    #         kwargs = msgdata['kwargs']
    #     )
    #     result = self.postprocess_message(result)
    #     return result