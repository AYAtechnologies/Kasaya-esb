#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
import os, codecs
from .parsers import load_settings_from_config_file, KasayaConfigParser, NoSectionError


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
    global settings
    from . import defaults
    exclude = set( ('absolute_import', 'division', 'print_function', 'unicode_literals') )
    # loading default settings
    for k,v in defaults.__dict__.items():
        if k in defaults.__builtins__:
            continue
        if k.startswith("__"):
            continue
        if k in exclude:
            continue
        settings[k] = v


def load_worker_settings(filename):
    """
    Simple function loading settings for service. Used for manual start of service.
    """
    env = {}
    conf = {}
    svc = {}
    cnf = KasayaConfigParser(filename)

    # service settings
    try:
        for k,v in cnf.items("service"):
            svc[k] = v
    except NoSectionError:
        pass

    # config
    try:
        for k,v in cnf.items("config"):
            conf[k] = v
    except NoSectionError:
        pass

    # environment
    try:
        for k,v in cnf.items("env"):
            env[k] = v
    except NoSectionError:
        pass

    return {'config':conf, 'service':svc, 'env':env}



#def load_config_from_file(filename, optional=False):
    #__load_config_section_from_file(filename, "config", optional, set_value )


load_defaults()
# load kasaya settings from system file
load_settings_from_config_file(SYSTEM_KASAYA_CONFIG, "config", True, set_value)

