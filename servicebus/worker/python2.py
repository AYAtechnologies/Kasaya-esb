#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
import sys
import socket
import fcntl
import struct
import array


def get_ip_for_nic(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(
        s.fileno(),
        0x8915,  # SIOCGIFADDR
        struct.pack(b'256s', ifname[:15])
    )[20:24])


def all_interfaces():
    is_64bits = sys.maxsize > 2**32
    struct_size = 40 if is_64bits else 32
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    max_possible = 8 # initial value
    while True:
        bytes = max_possible * struct_size
        names = array.array(b'B', b'\0' * bytes)
        outbytes = struct.unpack(b'iL', fcntl.ioctl(
            s.fileno(),
            0x8912,  # SIOCGIFCONF
            struct.pack(b'iL', bytes, names.buffer_info()[0])
        ))[0]
        if outbytes == bytes:
            max_possible *= 2
        else:
            break
    namestr = names.tostring()
    return [(namestr[i:i+16].split(b'\0', 1)[0],
             socket.inet_ntoa(namestr[i+20:i+24]))
            for i in range(0, outbytes, struct_size)]

