__author__ = 'wektor'

from servicebus.worker import WorkerDaemon

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


class Deamon(WorkerDaemon):
    def __init__(self):
        self.expose_all()

    def is_authorized_for_service(self, service, middleware):
        user, passwd = middleware["auth"]
        if DATA[user]["passwd"] == passwd and service in DATA[user]["services"]:
            return True
        else:
            return False

    def is_authorized_for_method(self, msg):
        return True
