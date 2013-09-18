#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import settings
from ConfigParser import SafeConfigParser
from kasaya.workers import launcher
import subprocess, sys, os, codecs
#!/usr/bin/env python




class Config(SafeConfigParser):

    def __init__(self, filename):
        SafeConfigParser.__init__(self,)
        with codecs.open(filename, "r", "utf_8") as f:
            self.readfp(f)

    def __getitem__(self, key):
        try:
            return SafeConfigParser.get(self, *key.split("|",1) )
        except:
            return None

    def __setitem__(self, key, value):
        sec,opt = key.split("|",1)
        try:
            SafeConfigParser.set(self, sec,opt, value )
            return
        except:
            self.add_section(sec)
            SafeConfigParser.set(self, sec,opt, value )

    #def get(key):
    #    try:
    #        SafeConfigParser.get(self, *key.split(":",1) )
    #    except:
    #        pass

    def getint(key):
        try:
            SafeConfigParser.getint(self, *key.split("|",1) )
        except:
            pass

    def getfloat(key):
        try:
            SafeConfigParser.getfloat(self, *key.split("|",1) )
        except:
            pass

    def getboolean(key):
        try:
            SafeConfigParser.getboolean(self, *key.split("|",1) )
        except:
            pass

    def getdate(self, key):
        d = self[key]
        if d is None:
            return None
        d = d.split('.',2)
        return datetime.date( int(d[2]), int(d[1]), int(d[0]) )


class Service(object):

    def __init__(self, directory):
        self.directory = os.path.join(settings.USER_WORKERS_DIR, directory)
        self.loc_conf = self.__get_service_local_config()
        self.name = self.loc_conf['service|name'].strip()
        self.mode = "python"

    def __get_service_local_config(self, configname="service.conf"):
        fn = os.path.join(self.directory, configname)
        return Config(fn)

    def __get_service_global_config(self, configname="services.conf"):
        fn = os.path.join(os.path.dirname(self.directory), configname)
        return Config(fn)

    def prepare(self):
        """
        Loads extra config before starting service
        """
        self.glob_conf = self.__get_service_global_config()


    def get_venv(self):
        """
        return path to virtualenv python or system python depending at config
        """
        # virtualenv name
        gvname = self.glob_conf[self.name+"|venv_name"]
        vname = self.loc_conf['virtualenv|name']
        if not gvname is None:
            vname=gvname

        # virtualenv directory
        # gobal default venv directory
        vdir = self.glob_conf["default|venv_dir"]
        # local venv directory
        v = self.loc_conf['virtualenv|dir']
        if not v is None:
            vdir = v
        # overriden service venv in global config
        v = self.glob_conf[self.name+"|venv_dir"]
        if not v is None:
            vdir = v

        # no vanenv dir
        if vdir is None:
            if not vname is None:
                raise Exception("virtualenv name is defined, but directory is missing")
            return None

        # only directory
        if vname is None:
            return vdir

        # dir and subdir (venv name)
        return os.path.join(vdir, vname)


    def get_python_cmd(self):
        venv = self.get_venv()
        if venv is None:
            return "python"
        else:
            venv = os.path.abspath(venv)
            cmd = os.path.join(venv, "bin","python" )
            if not os.path.exists(cmd):
                raise Exception("virtualenv python not found: %s" % venv)
            return cmd


    def get_environment(self):
        env = os.environ.copy()

        # default environment settings
        for k,v in self.glob_conf.items("default"):
            if not k.startswith("env."):
                continue
            k = k[4:]
            if len(k)>0:
                env[k] = v

        # local environment settings
        for k,v in self.loc_conf.items("env"):
            env[k] = v

        # global environment settings
        for k,v in self.glob_conf.items(self.name):
            if not k.startswith("env."):
                continue
            k = k[4:]
            if len(k)>0:
                env[k] = v

        # module name
        modname = self.loc_conf['service|module']
        if modname.endswith(".py"):
            modname = modname[:-3]
        env['SV_MODULE_IMPORT'] = modname

        # service name
        env['SV_SERVICE_NAME'] = self.name
        return env


    def start_service_python(self):
        cmd = [ self.get_python_cmd() ]
        cmd.append( launcher.__file__ )

        env = self.get_environment()
        po = subprocess.Popen(
            cmd,
            cwd=self.directory,
            env=env,
            #stdout=subprocess.PIPE,
            #stderr=subprocess.PIPE,
        )
        #po.wait()
        print po



def find_local_services():
    """
    Return list of local services
    """
    svlist = []
    def addsv(name, dirname=None, config=None):
        d = {'service':name, 'dir':dirname, 'config':config}
        svlist.append(d)

    # embedded services
    if settings.USE_ASYNC_SERVICE:
        addsv(settings.ASYNC_DAEMON_SERVICE)

    if settings.USE_TRANSACTION_SERVICE:
        addsv("transd")

    if settings.USE_AUTH_SERVICE:
        addsv("authd")

    if settings.USE_LOG_SERVICE:
        addsv("logd")

    # user services
    dname = os.path.abspath( settings.USER_WORKERS_DIR )
    if not os.path.exists(dname):
        return svlist

    for sdir in os.listdir( dname ):
        fp = os.path.join(dname, sdir)
        if not os.path.isdir(fp):
            continue
        # is configuration file
        cnf = os.path.join( fp, "service.conf" )
        if not os.path.exists(cnf):
            continue

        # is service worker
        addsv(sdir, fp, cnf)

    return svlist



if __name__=="__main__":
    #wlst = find_local_services()
    #print wlst
    s = Service('/home/moozg/services/myservice')
    s.prepare()
    s.start_service_python()
    #print s.get_service_global_config()
    #get_service_config('/home/moozg/services/simple')
    #run_service_worker('/home/moozg/services/simple')
