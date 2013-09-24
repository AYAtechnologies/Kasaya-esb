#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import settings, SYSTEM_SERVICES_CONFIG, SERVICE_CONFIG_NAME, SERVICE_GLOBAL_CONFIG_NAME
from kasaya.conf.parsers import CombinedConfig, KasayaConfigParser
from kasaya.core.lib import LOG
from kasaya.workers import launcher
import subprocess, os
from ConfigParser import SafeConfigParser, NoSectionError



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
        CNF.service(self.name) # change parser mode
        self.mode = "python" # only python mode is possible for now
        self.module = CNF['service|module']

        # virtualenv dir
        vdir = CNF["virtualenv|dir"]
        vname = CNF["virtualenv|name"]
        if vdir is None:
            self.venv = None
        else:
            if vname is None:
                self.venv = vdir
            else:
                self.venv = os.path.join(vdir, vname)

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
        self.env['SV_MODULE_IMPORT'] = self.module.rstrip(".py")


    def get_python_cmd(self):
        if self.venv is None:
            return "python"
        else:
            self.venv = os.path.abspath(self.venv)
            cmd = os.path.join(self.venv, "bin","python" )
            if not os.path.exists(cmd):
                raise Exception("virtualenv python not found: %s" % self.venv)
            return cmd


    def start_service(self):
        cmd = [ self.get_python_cmd() ]
        cmd.append( launcher.__file__ )
        po = subprocess.Popen(
            cmd,
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

        # is service worker
        s = Service(fp)
        if s.name in result:
            LOG.error("Found more than one service with name [%s]. Ignoring." % s.name)
            continue
        result[s.name] = s

    return result

