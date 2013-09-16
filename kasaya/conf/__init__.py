#coding: utf-8
import os
import defaults

__all__ = ("settings", "load_config_from_file")

class SettingsProxy(dict):
    def __getattr__(self, k):
        return self[k]

settings = SettingsProxy()


SYSTEM_CONF_FILE = "/etc/kasaya/kasaya.conf"


def _parse_config(filename):
    """
    Simple text config parser
    """
    res = {}
    with file(filename,"r") as cnf:
        for ln in cnf.readlines():
            ln=ln.strip()
            if len(ln)<2: continue
            if ln.startswith("#"): continue
            try:
                k,v = ln.split("=",1)
            except ValueError:
                continue
            res[k.rstrip()] = v.strip()
    return res


def load_config_from_file(filename, optional=False):
    """
    Load config and change values to types used in default settings
    """
    try:
        cnf = _parse_config(filename)
    except IOError as e:
        if optional:
            print "Optional config file [%s] not exists. Skipping." % filename
            return
        else:
            print "Config file [%s] not exists. Stopping." % filename
            import sys
            sys.exit(1)

    for k,v in cnf.iteritems():
        if k in settings:
            typ = type( settings[k] )
            # boolean has some special values
            if typ is bool:
                settings[k] = v.lower() in ("1", "tak", "y", "yes","true")
            else:
                settings[k] = typ(v)
        else:
            settings[k] = v



# loading default settings
for k,v in defaults.__dict__.iteritems():
    if k in defaults.__builtins__:
        continue
    if k.startswith("__"):
        continue
    settings[k] = v

# loading system settings
load_config_from_file(SYSTEM_CONF_FILE, optional=True)

