#!/usr/bin/env python
#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
import sys, os



def close_all_fds():
    """
    Theoretically this should be called when daemonizing process,
    but it just kills daemon without any notification what goes wrong,
    so we never use this function in our code.
    """
    import resource
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
    this function is based on this receipe:
    http://code.activestate.com/recipes/278731-creating-a-daemon-the-python-way/
    """
    # first fork
    pid = os.fork()
    if (pid == 0): # first child.
        os.setsid()
        pid = os.fork() # second child.
        if (pid == 0): # im second child.
            os.umask(UMASK)
        else:
            os._exit(0)
    else:
        os._exit(0)
    # close_all_fds() # <- this function will kill your daemon silently
    return 0



if __name__=="__main__":

    try:
        retCode = createDaemon()
    except IOError as e:
        # daemon creation failed
        sys.exit(1)

    # what to run...
    servicename = os.environ['SV_SERVICE_NAME']
    module = os.environ['SV_MODULE_IMPORT']
    try:
        kasayad_mode = os.environ['SV_KASAYAD_MODE'].strip().lower().startswith("y")
    except:
        kasayad_mode = False
    import sys

    # worker settings
    from kasaya.conf import set_value, settings

    # additional settings
    for k,v in os.environ.items():
        if k.startswith("SV_CNF_"):
            k = k[7:]
            if len(k)>1:
                set_value(k, v)

    # setup logging
    #if not settings.LOG_TO_FILE:
    set_value("LOG_TO_FILE", "1")
    set_value("LOGGER_NAME", servicename )
    set_value("LOG_FILE_NAME", os.environ.get('SV_LOG_FILE', "/tmp/service_%s.log" % servicename) )
    from kasaya.core.lib.logger import stdLogOut
    from kasaya.core.lib import LOG

    # redirect stdout and stderr to log
    sys.stdout = stdLogOut(LOG, "DEBUG")
    sys.stderr = stdLogOut(LOG, "ERROR")

    LOG.stetupLogger()
    if kasayad_mode:
        LOG.info("KASAYAAAAA")
        # starting kasaya daemon
        from kasaya.workers.kasayad import KasayaDaemon
        daemon = KasayaDaemon()
        daemon.run()
    else:
        # starting regular worker
        from kasaya import WorkerDaemon
        cwd = os.getcwd()
        if not cwd in sys.path:
            sys.path.append(cwd)
        __import__(module)

        worker = WorkerDaemon(servicename)
        worker.run()

