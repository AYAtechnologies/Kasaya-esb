#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.core.worker import WorkerDaemon
import sys, os, resource



def close_all_fds():
    maxfd = resource.getrlimit(resource.RLIMIT_NOFILE)[1]
    if (maxfd == resource.RLIM_INFINITY):
        maxfd = MAXFD
    for fd in xrange(0, maxfd):
        try:
            os.close(fd)
        except OSError:
            pass


def createDaemon(UMASK=0, MAXFD=1024):
    """
    function is based on this receipe:
    http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/
    """
    # first fork
    pid = os.fork()
    if (pid == 0):  # first child.
        os.setsid()
        pid = os.fork()  # second child.
        if (pid == 0): # second child.
            os.umask(UMASK)
        else:
            os._exit(0)
    else:
        os._exit(0)
    # close_all_fds() # <- this function kill daemon
    return 0




if __name__=="__main__":
    try:
        retCode = createDaemon()
    except IOError as e:
        # daemon creation failed
        sys.exit(1)

    #from kasaya.core.lib.logger import stdLogOut
    #import logging
    #LOG = logging.getLogger("launcher")
    #LOG.setLevel(logging.DEBUG)
    #ch = logging.FileHandler("/tmp/daemonize.log", mode='a+', encoding="utf-8")
    #formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    #ch.setFormatter(formatter)
    #LOG.addHandler(ch)
    #del formatter, ch
    #sys.stdout = stdLogOut(LOG)
    #sys.stderr = stdLogOut(LOG)

    servicename = os.environ['SV_SERVICE_NAME']
    module = os.environ['SV_MODULE_IMPORT']

    cwd = os.getcwd()
    if not cwd in sys.path:
        sys.path.append(cwd)
    __import__(module)

    worker = WorkerDaemon(servicename)
    worker.run()

