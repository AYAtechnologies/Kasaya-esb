#coding: utf-8

class SettingsProxy(dict):
    def __getattr__(self, k):
        return self[k]


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

def load_config_from_file(filename):
    """
    Load config and change valuse to types used in default settings
    """
    for k,v in _parse_config(filename).iteritems():
        if k in settings:
            typ = type( settings[k] )
            settings[k] = typ(v)
        else:
            settings[k] = v

# load default settings
settings = SettingsProxy()

import defaults
for k,v in defaults.__dict__.iteritems():
    if k in defaults.__builtins__:
        continue
    if k.startswith("__"):
        continue
    settings[k] = v

