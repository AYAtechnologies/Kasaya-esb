#!/usr/bin/env python
#coding: utf-8
from servicebus import protocol
from servicebus.client.queries import SyncDQuery


def execute_sync_task(method, authinfo, timeout, args, kwargs):
    """
    Wywołanie synchroniczne jest wykonywane natychmiast.
    """
    print "SYNCHRONOUS",
    print "AUTHINFO:", authinfo,  "TIMEOUT:", timeout,
    print "method:", method
    worker = SyncDQuery.query( method[0] )
    #    print "Worker:",worker
    print "ARGS:",args,
    print "KWARGS:",kwargs
    print


def register_async_task(method, authinfo, timeout, args, kwargs):
    """
    Wywołanie asynchroniczne powinno zostać zapisane w bazie i zostać wykonane
    w tle. Wynikiem funkcji powinien być identyfikator zadania wg którego można
    sprawdzić jego status.
    """
    print "ASYNCHRONOUS",
    print "AUTHINFO:", authinfo, "TIMEOUT:", timeout,
    print "method:", method
    worker = SyncDQuery.query( method[0] )
#    print "Worker:",worker
    print "ARGS:",args,
    print "KWARGS:",kwargs
    print
#    return "fake-id"
