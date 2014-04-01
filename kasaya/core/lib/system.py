#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from socket import gethostname
import resource

__all__=("get_memory_used", "get_hostname", "all_interfaces")


def get_memory_used():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

def get_hostname():
    return gethostname()


def all_interfaces():
    """
    Return list of interfaces and associated IP addresses

    """
    import netifaces
    res = {}
    for ifc in netifaces.interfaces():
        try:
            ip_list = netifaces.ifaddresses(ifc)[netifaces.AF_INET]
            if len(ip_list) > 1:
                raise NotImplemented("Only one IP per interface supported")
            res[ifc] = ip_list[0]["addr"] #IP info also available: "mask" "broadcast"
        except KeyError:
            #interface is not supporting IP address
            pass
    return res
