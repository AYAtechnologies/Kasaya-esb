#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from zmq.core.error import ZMQError
from servicebus.conf import settings
import netifaces
import sys, socket, fcntl, struct, array



def get_ip_for_nic(ifname):
    """
    Get IP for network interface (eth0, eth1, lo,... )
    http://code.activestate.com/recipes/439094-get-the-ip-address-associated-with-a-network-inter/
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack(b'256s', ifname[:15])
    )[20:24])


def all_interfaces():
    """
    Return list of interfaces and associated IP addresses

    """
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
    print res
    return res


def get_bind_address(bindto=None):
    """
    Returns address to bind socket as specified in configuration
    """
    if bindto is None:
        bindto = settings.BIND_WORKER_TO
    avail = all_interfaces()
    # network interface name
    if bindto in avail:
        return avail[bindto]
    # local network
    if bindto=="LOCAL":
        return avail['lo']
    # normal internet interface
    if bindto=="AUTO":
        for n, ip in avail.items():
            if n=="lo": continue
            if ip.startswith("127."): continue
            return ip
    # jeśli nic nie pasuje, to ustawienia z settings są przekazywane bez zmian
    return bindto



def bind_socket_to_port(zsock, port):
    myip = get_bind_address()
    addr = "tcp://%s:%i" % (myip, port)
    zsock.bind(addr)
    return addr



def bind_socket_to_port_range(zsock, p1, p2):
    """
    Binds to first available port configured in settings
    """
    myip = get_bind_address()
    minport, maxport = min(p1,p2), max(p1,p2)

    port = minport
    while True:
        addr = "tcp://%s:%i" % (myip, port)
        print "connecting: ",addr,
        try:
            zsock.bind(addr)
            print "success"
            break
        except ZMQError:
            print "fail"
            pass
        port += 1
        if port>maxport:
            raise Exception("Can't find available port for worker, aborting.")
    return addr

