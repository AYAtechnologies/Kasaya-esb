Kasaya-ESB
==========

Enterprise service-bus with distributed transactions.

## Status
At the moment library is unstable


Installation
============

Installation using pip from github repository:

    pip install git+https://github.com/AYAtechnologies/Kasaya-esb.git

To install some of required extensions development libraries and additional packages are required. On debian systems use apt-get to install requirements:

    apt-get install libzmq-dev cython libevent-dev g++ python-dev

Python requirements are automatically installed when using pip to install Kasaya-ESB:

    pyzmq
    gevent
    msgpack-python
    netifaces
    pycrypto
    plac


Installation for Python 3
==========================

system dependencies

    apt-get install python3 python3-setuptools python3-pip

Optionally install virtualenv

    pip3 install virtualenv

Python modules. Until gevent will not support python 3, we use fantix's fork of gevent which work witch python 3.

    netifaces-py3
    greenlet
    pyzmq
    git+https://github.com/fantix/gevent.git
    plac
    pycrypto
    msgpack-python


Configuration
=============

All workers and core components are configured via kasaya.conf module.

Example of reading settings:

    from kasaya.conf import settings
    print settings.USER_WORKERS_DIR

Global Kasaya settings are stored in /etc/kasaya/kasaya.conf file. This file is loaded automatically when Kasaya is starting, and for each managed service.

It's possible to load own settings manually using load_config_from_file function:

    from kasaya.conf import settinsg, load_config_from_file
    load_config_from_file("my_own_config.conf")
    print settings.USER_WORKERS_DIR

All automatically managed services requires own setting file in main directory. Name of this file is "service.conf" and it's mandatory to treat this directory by syncd server as service.






