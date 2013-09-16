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

    zerorpc
    gevent
    gevent-zeromq
    msgpack-python
    netifaces
    pycrypto
    plac


---

### fast
 - lightweight
 - easy to implement and to expose written code (no more than writing a 10 line worker class)
 - linear scalability

### distributed
 - No global database
 - No configuration change needed to add workers (all workers should have common configuration)
 - any number of workers serving the same service should be possible
 - distributed global configuration and broadcasting
 - no single point of failure

### service-oriented
 - service discovery
 - each feature of the system should be able to be run on any worker
 - workers would serve "system" services and "application" services

### layered architecture
 - each message would be processed by layered "application" services eg. authorization

### persistent
 - failure of a single worker should not stop the system
 (it may stop providing the service )
 - failure of data stored by a configurable amount of workers should not cause any data loss in the system
 - persistence based on key-value stores held on each worker

### all synchronization support
 - synchronous
 - asynchronous with no response (task)
 - asynchronous with polling

### Good exception propagation
### independent
 - limited requirements (ZMQ, )
 - no persistance dependency any key-value store should be enough to run (first implementation dependend on Riak)