#!/usr/bin/env python
#coding: utf-8
from __future__ import unicode_literals
from servicebus.lib.comm import send_and_receive
from servicebus import exceptions
from servicebus.lib import LOG
from servicebus.protocol import messages


__all__=("RedirectRequired", "ControlTasks")


class RedirectRequiredToIP(Exception):
    def __init__(self, ip):
        self.remote_ip = ip

class RedirectRequiredToAddr(Exception):
    def __init__(self, addr):
        self.remote_addr = addr


class ControlTasks(object):

    def __init__(self, context, allow_redirect=False):
        self.__ctltasks = {}
        self.__allow_redirect = allow_redirect
        self.__context = context


    def register_task(self, method, func):
        """
        Register control method.
        """
        self.__ctltasks[method] = func


    def ip_to_zmq_addr(self, ip):
        raise NotImplemented

    def redirect(self, addr, message):
        # If function can't process this request, it should be redirected
        # to another host to process. Valid IP for request is stored
        # in exception remote_ip field
        if not self.__allow_redirect:
            raise ServiceBusException("Message redirection is not allowed here")
        if 'redirected' in message:
            # this message is coming from another sync daemon
            # we allow only one redirection and if message is still
            # delivered to wrong host, then we drop message
            raise ServiceBusException("Message redirection fail")
        message['redirected'] = True
        return send_and_receive(self.__context, addr, message, settings.SYNC_REPLY_TIMEOUT)

    def handle_request(self, message):
        """
        All control requests are handled here.
           message - message body
           islocal - if true then request is from localhost
        """
        method = ".".join(message['method'])
        LOG.debug("Management call [%s]" % method)
        #LOG.debug(repr(message))

        try:
            func = self.__ctltasks[method] # get handler for method
        except KeyError:
            raise exceptions.MethodNotFound("Control method %s not exists" % method)

        # fill missing parameters
        if not 'args' in message:
            message['args'] = []
        if not 'kwargs' in message:
            message['kwargs'] = {}

        # call internal function
        try:
            result = func(*message['args'], **message['kwargs'])
        except RedirectRequiredToIP as e:
            addr = self.ip_to_zmq_addr(e.remote_ip)
            return self.redirect(addr, message)
        except RedirectRequiredToAddr as e:
            return self.redirect(e.remote_addr, message)

        return {"message":messages.RESULT, "result":result }

