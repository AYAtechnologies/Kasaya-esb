#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from kasaya.conf import settings
from gevent import socket
import resource


def get_memory_used():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

def get_hostname():
    return socket.gethostname()


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
