__author__ = 'wektor'

import os, sys
from servicebus.middleware.demon import MiddlewareDemon

esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(esbpath)
from servicebus.conf import load_config_from_file

DATA = {
    "admin":
        {
            "passwd": "pass",
            "services": ["async_demon", "fikumiku"]
        },
    "sync":
        {
            "passwd":"dupa",
            "services":["fikumiku"]
        }
}


class Demon(MiddlewareDemon):
    def __init__(self, name):
        super(Demon, self).__init__(name)
        self.expose_all()

    def is_authorized_for_service(self, service, context):
        print service, context
        if context is None:
            return False
        user, passwd = context["auth"]
        if DATA[user]["passwd"] == passwd and service in DATA[user]["services"]:
            return True
        else:
            return False

    def is_authorized_for_method(self, msg):
        return True


if __name__=="__main__":
    load_config_from_file("../../config.txt")
    demon = Demon("auth")
    demon.run()