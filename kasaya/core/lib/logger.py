#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import settings
import logging, sys



class stdLogOut(object):
    """
    Catch std out/err and redirects it to log
    """
    def __init__(self, logger, level=logging.DEBUG):
        self.logger = logger
        self.level = level

    def write(self, msg):
        msg = msg.strip()
        if len(msg)>0:
            self.logger.log(self.level, msg)
    def flush(self):
        for handler in self.logger.handlers:
            handler.flush()



def setup_logging(name="svbus"):
    logger = logging.getLogger(name)
    # log level
    ll = settings.LOG_LEVEL.upper()
    levels = {
        'DEBUG':logging.DEBUG,
        'INFO':logging.INFO,
        'WARNING':logging.WARNING,
        'ERROR':logging.ERROR,
        'CRITICAL':logging.CRITICAL,
    }
    try:
        logger.setLevel(levels[ll])
    except KeyError:
        raise Exception ("Invalid log level in config %s" % ll)

    # wyjście
    if settings.LOG_TO_FILE:
        # logowanie do pliku
        ch = logging.FileHandler(settings.LOG_FILE_NAME, encoding="utf-8")
    else:
        # logowanie na wyjście
        ch = logging.StreamHandler(stream=sys.stderr)

    # format wyjścia
    formatter = logging.Formatter('%(asctime)s [%(name)s] %(levelname)s: %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    return logger


LOG = setup_logging()
