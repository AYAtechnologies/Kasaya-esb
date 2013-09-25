#coding: utf-8
import __future__
import os, codecs
from .parsers import load_config_file as __load_config_file


SERVICE_CONFIG_NAME = "service.conf"
SERVICE_GLOBAL_CONFIG_NAME = "services.conf"
SYSTEM_KASAYA_CONFIG = "/etc/kasaya/kasaya.conf"
SYSTEM_SERVICES_CONFIG = "/etc/kasaya/"+SERVICE_GLOBAL_CONFIG_NAME


__all__ = ("settings", "load_config_from_file")


class SettingsProxy(dict):
    def __getattr__(self, k):
        return self[k]

settings = SettingsProxy()



def set_value(k,v):
    global settings
    k=k.strip().upper().replace(" ","_")
    if k in settings:
        typ = type( settings[k] )
        # boolean has some special values
        if typ is bool:
            settings[k] = v.lower() in ("1", "tak", "y", "yes","true")
        else:
            settings[k] = typ(v)
    else:
        settings[k] = v



def load_defaults():
    from . import defaults
    global settings
    # loading default settings
    for k,v in defaults.__dict__.items():
        if k in defaults.__builtins__:
            continue
        if k.startswith("__"):
            continue
        settings[k] = v



def load_config_from_file(filename, optional=False):
    __load_config_file(filename, "config", optional, set_value )


load_defaults()
load_config_from_file(SYSTEM_KASAYA_CONFIG, optional=True)

