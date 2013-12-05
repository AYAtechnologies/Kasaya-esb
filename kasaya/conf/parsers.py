#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
import os, codecs
try:
    from ConfigParser import SafeConfigParser, NoSectionError
except ImportError:
    from configparser import SafeConfigParser, NoSectionError



class KasayaConfigParser(SafeConfigParser):

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



class CombinedConfig(object):

    def __init__(self, servicename=None):
        self.confs = []
        self.svc = servicename

    def service(self, name):
        self.svc = name

    def load_config(self, filename, globmode, optional=False, verbose=True):
        """
        filename - name of config file to load
        globmode - if True, global section names [config], [env]...
                   if False, local workers sections: [xxxx:config], [xxxx:env]...
        optional - if set to True then no exception will be raised if file does not exist
        verbose - some additional log information will be displayed
        """
        try:
            cnf = KasayaConfigParser(filename)
        except IOError as e:
            if optional:
                if verbose:
                    print ("Optional config file [%s] not exists. Skipping." % filename)
                return False
            else:
                raise e
        self.confs.append( (cnf, globmode) )
        return True

    def __itervals(self, k):
        for cnf, gmode in self.confs:
            yield cnf[k] # global section or normal config for one service
            if gmode and self.svc:
                yield cnf[self.svc+":"+k] # service specific section

    def __getitem__(self, k):
        res = None
        for v in self.__itervals(k):
            if v is None:
                continue
            res = v
        return res

    def options(self, section):
        known=set()
        for cnf, gmode in self.confs:
            # global section or services own configs
            try:
                for opt in cnf.options(section):
                    if opt not in known:
                        known.add(opt)
                        yield opt
            except NoSectionError:
                pass
            # service specific section
            if gmode and self.svc:
                try:
                    for opt in cnf.options(self.svc+":"+section):
                        if opt not in known:
                            known.add(opt)
                            yield opt
                except NoSectionError:
                    pass

    def items(self, section):
        for opt in self.options(section):
            yield opt, self.__getitem__(section+"|"+opt)



def load_settings_from_config_file(filename, section, optional, set_value, verbose=True, add_prefix=""):
    """
    Load config and change values to types used in default settings
    """
    try:
        cnf = KasayaConfigParser(filename)
    except IOError as e:
        if optional:
            if verbose:
                print ("Optional config file [%s] not exists. Skipping." % filename)
            return False
        else:
            if verbose:
                print ("Config file [%s] not exists. Stopping." % filename)
            import sys
            sys.exit(1)

    for k,v in cnf.items(section):
        set_value(add_prefix+k,v)

    return True
