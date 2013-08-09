#!/usr/bin/env python
#coding: utf-8
#import settings
#import zmq
#from protocol import serialize, deserialize


class Task(object):

    def __init__(self):
        pass

    def __call__(self, func):

        return func # nie dekorujemy

        def wrap(request, *args, **kwargs):
            return func(request, *args, **kwargs)

        return wrap
