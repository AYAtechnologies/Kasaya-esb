#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import settings, SYSTEM_SERVICES_CONFIG, SERVICE_CONFIG_NAME, SERVICE_GLOBAL_CONFIG_NAME
from kasaya.conf.parsers import CombinedConfig, KasayaConfigParser
from kasaya.core.lib import LOG
from kasaya.workers import launcher
import subprocess, os
from ConfigParser import SafeConfigParser, NoSectionError


class UnknownServiceMode(Exception): pass


class Service(object):

    def __init__(self, directory):
        # worker directory
        self.directory = os.path.join(settings.USER_WORKERS_DIR, directory)

        # load config
        CNF = CombinedConfig()
        CNF.load_config(
            SYSTEM_SERVICES_CONFIG,
            globmode=True, optional=True )
        CNF.load_config(
            os.path.join(self.directory, SERVICE_CONFIG_NAME),
            globmode=False, optional=False )
        CNF.load_config(
            os.path.join(settings.USER_WORKERS_DIR, SERVICE_GLOBAL_CONFIG_NAME),
            globmode=True, optional=True)

        # service name and module
        self.name = CNF['service|name']
        self.mode = CNF['service|mode']
        # language
        if self.mode in (None, "python"):
            self.mode="python2"
        elif self.mode!="python3":
            e = UnknownServiceMode()
            e.service = self.name
            e.mode = self.mode
            raise e
        CNF.service(self.name) # change parser mode
        self.module = CNF['service|module']


        if self.mode.startswith("python"):
            # virtualenv dir is used only with python
            vdir = CNF["virtualenv|dir"]
            vname = CNF["virtualenv|name"]
            if vdir is None:
                self.venv = None
            else:
                if vname is None:
                    self.venv = vdir
                else:
                    self.venv = os.path.join(vdir, vname)
            # service name for python
            self.module = self.module.rstrip(".py")

        # environment variables
        self.env = {}
        for k,v in CNF.items("env"):
            k = k.replace(" ","_")
            self.env[k]=v

        # override config variables
        for k,v in CNF.items("config"):
            k = k.replace(" ","_").upper()
            self.env["SV_CNF_"+k] = v

        # special settings
        self.env['SV_SERVICE_NAME'] = self.name
        self.env['SV_MODULE_IMPORT'] = self.module


    def get_start_command(self):
        if self.mode.startswith("python"):
            # python interpreter
            pyname = self.mode
            if self.venv is None:
                cmd = pyname
            else:
                self.venv = os.path.abspath(self.venv)
                cmd = os.path.join(self.venv, "bin",pyname )
                if not os.path.exists(cmd):
                    raise Exception("virtualenv python not found: %s" % self.venv)
            # launcher file
            lfile = launcher.__file__
            if lfile[-4:] in (".pyc",".pyo"):
                lfile = lfile[:-4] + ".py"
            return [cmd, lfile]
        else:
            raise NotImplemented("this should never occur")




    def start_service(self):
        po = subprocess.Popen(
            self.get_start_command(),
            cwd=self.directory,
            env=self.env,
            #stdout=subprocess.PIPE,
            #stderr=subprocess.PIPE,
            close_fds=True, # <--- very important!
        )




class InternalService(Service):

    def __init__(self, name, globalcnf):
        self.name = name
        self.directory = os.path.dirname(launcher.__file__)
        self.mode = "python"
        #self.directory =

    def get_environment(self):
        env = os.environ.copy()
        env['SV_MODULE_IMPORT'] = self.name
        env['SV_SERVICE_NAME'] = self.name


    def get_python_cmd(self):
        return "python"


def local_services():
    """
    Return list of local services
    """
    #cnfame = os.path.join( settings['USER_WORKERS_DIR'], SERVICE_GLOBAL_CONFIG_NAME )
    #if os.path.exists(cnfame):
    #    config = KasayaConfigParser(cnfame)
    #else:
    #    config = None
    result = {}

#    # internal services
#    if settings.USE_ASYNC_SERVICE:
#        s = Service("asyncd", config)
#        result[s.name] = s
#
#    if settings.USE_TRANSACTION_SERVICE:
#        s = Service("transactiond", config)
#        result[s.name] = s
#
#    if settings.USE_AUTH_SERVICE:
#        s = Service("authd", config)
#        result[s.name] = s
#
#    if settings.USE_LOG_SERVICE:
#        s = Service("logd", config)
#        result[s.name] = s

    # user services
    dname = os.path.abspath( settings.USER_WORKERS_DIR )
    if not os.path.exists(dname):
        return result

    for sdir in os.listdir( dname ):
        # service config
        fp = os.path.join(dname, sdir)
        cnf = os.path.join( fp, SERVICE_CONFIG_NAME )
        if not os.path.exists(cnf):
            continue

        # directory contains service
        try:
            s = Service(fp)
        except UnknownServiceMode as e:
            LOG.error("Service [%s] requires mode [%s] which is unimplemented. Ignoring." % (e.service, e.mode) )
            continue

        if s.name in result:
            LOG.error("Found more than one service with name [%s]. Ignoring." % s.name)
            continue

        result[s.name] = s

    return result

