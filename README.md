Kasaya-ESB
==========

Enterprise service-bus with distributed transactions.

Kasaya is written using Python 2.7. It's now possible to create services using Python 3. All internal services are python 2.7 compatible, client library is not yet ready for use under Python 3.

Warning: Python 3 compatibility is experimental, because of gevent module, which is officially not ported to Python 3. In this version of python we use unoficcial gevent fork.


### Status
At the moment library is **unstable**.


Using Kasaya
============

Kasaya is designed to be as simple as possible and hide all complicated network and management tasks from user.

**Creating simple worker**

    from Kasaya import Task

    @Task(name="heavytask")
    def my_heavy_task(param):
        print "doing something..."
        print param

    @Task(name="other_task")
    def task2(param, *args, **kwargs):
        print args
        print kwargs
        return "some result"


We assume that our service will be visible in Kasaya under **myservice** name.

**Using our service**

    from Kasaya import sync

    print sync.myservice.my_heavy_task(123)
    print sync.myservice.other_task(1, True, 123, foo="foo", baz="bar")


Where sync means that our tasks will be called immediatelly. We can also call our tasks asynchronous. Just chande *sync* to *async*:

    from Kasaya import async

    res = async.myservice.my_heavy_task(123)
    res = async.myservice.other_task(1, True, 123, foo="foo", baz="bar")

To run our service You need configured and started kasaya daemon on each host with working services and clients using them.

to make our service fully working save our python module for example as *myservice_code.py* and create additional file named *service.conf* with content:

    [service]
    name = myservice
    module = myservice_code

both files save to directory named myservice (You can name this director as You want, but using same name like name of service is recommended).

All services should be stored in directory indicated in */etc/kasaya/kasaya.conf* file in *LOCAL_WORKERS_DIR* setting.

Default it is '/opt/services' directory.

Your directory layout should look like this:

    /opt
      /services
        /myservice
          myservice_code.py
          service.conf

Then use kasaya's command to rescan services:

    svcbus rescan

If everything is ok your worker should be available to start now. To start it use command:

    svbus svcstart myservice

To see if is Your worker active list kasaya network:

   svbus list

You should see:

        SYNCD  192.168.0.201 (hostname: myubuntu)
    myservice  on port: 5000

Now You can run client code and check if the service is working.



Installation for Python 2
=========================

Installation using pip from github repository:

    pip install git+https://github.com/AYAtechnologies/Kasaya-esb.git

To install some of required extensions development libraries and additional packages are required. On debian systems use apt-get to install requirements:

    apt-get install libzmq-dev cython libevent-dev g++ python-dev

Python requirements are automatically installed when using pip to install Kasaya-ESB:

    pyzmq
    gevent
    netifaces
    pycrypto
    plac
    msgpack-python
    bson

Packages msgpack-python and bson are required if You choose BSON or messagepac as your transport protocol.



Installation for Python 3
==========================

To install kasaya and dependencies You need some system components. Under debian family systems You can install them using apt-get command:

    apt-get install python3 python3-setuptools python3-pip

Optionally install virtualenv:

    pip3 install virtualenv

Python 3 requirments installed with Kasaya:

    netifaces-py3
    greenlet
    pyzmq
    git+https://github.com/fantix/gevent.git
    plac
    pycrypto
    msgpack-python

Module msgpack-python is not required if You choose different transport protocol in configuration.



Configuration
=============

All workers and core components are configured using kasaya.conf module.

Example of reading settings:

    from kasaya.conf import settings
    print settings.USER_WORKERS_DIR

Global Kasaya settings are stored in /etc/kasaya/kasaya.conf file. This file is loaded automatically when Kasaya and every service is starting.

It's possible to load own settings manually using load_config_from_file function:

    from kasaya.conf import settinsg, load_config_from_file
    load_config_from_file("my_own_config.conf")
    print settings.USER_WORKERS_DIR

All automatically managed services requires own setting file in main directory. Name of this file is "service.conf" and it's mandatory to treat this directory by syncd server as service.
