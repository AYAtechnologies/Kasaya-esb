__author__ = 'wektor'

import os, sys
from servicebus.worker import Daemon

esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(esbpath)
from servicebus.conf import load_config_from_file

class Demon(Daemon):
    def __init__(self, name):
        super(Demon, self).__init__(name)
        self.expose_all()

    def log_client_call(self, message):
        print "log_client_call", message

    def log_worker_received(self, message):
        print "log_worker_received", message

    def log_worker_respond(self, message):
        print "log_worker_respond", message

    def log_client_received(self, message):
        print "log_client_received", message


if __name__=="__main__":
    load_config_from_file("../../config.txt")
    demon = Demon("logging")
    demon.run()