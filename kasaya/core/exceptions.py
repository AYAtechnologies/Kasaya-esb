#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals


class ExtendedInfo(object):

    def info(self):
        try:
            remote = self.remote
        except AttributeError:
            remote = False

        if remote:
            res = "REMOTE EXCEPTION\n"
        else:
            res = "LOCAL EXCEPTION\n"

        try:
            rp = self.request_path
            res += "Request path:\n"
            for i,p in enumerate(self.request_path):
                res += "{0: >2} - {1}\n".format(i+1,p)
        except AttributeError:
            pass

        try:
            tb = self.traceback
        except AttributeError:
            tb = None
        if tb is None:
            tb = "No traceback found"
        res += tb
        return res



class RemoteException(Exception, ExtendedInfo):
    """
    All exceptions raised by remote worker will be locally raised as RemoteException
    """
    pass


class ServiceBusException(Exception, ExtendedInfo):
    """
    All internal exceptions are inherited from this class
    """
    pass



class SerializationError(ServiceBusException):
    """
    Serialization of data to transfer fails. Probably used transport desn't support used data type.
    """
    pass

class NetworkError(ServiceBusException):
    """
    Any network level error during message transmission
    """
    pass


class NotOurMessage(ServiceBusException):
    """
    Incoming message is coming from other service bus network.
    """
    pass

class ReponseTimeout(ServiceBusException):
    pass

class MessageCorrupted(ServiceBusException):
    """
    Nadchodzączy komunikat jest uszkodzony, lub rozszyfrowanie się nie powiodło.
    """
    pass


class MaximumDepthLevelReached(ServiceBusException):
    """
    Maximum depth of requests reached
    """
    pass

#class ExecutionRejected(ServiceBusException):
#    """
#    Wykonanie metody odrzucone z powodu braku uprawnień.
#    """
#    pass

#class AnonymousExecutionDisabled(ExecutionRejected):
#    """
#    Wykonanie metody anonimowo jest zabronione
#    """
#    pass

#class NotEnoughPriviliges(ExecutionRejected):
#    """
#    Niewystarczajace uprawnienia do wykonania metody
#    """
#    pass


class ServiceNotFound(ServiceBusException):
    """
    Żądany serwis nie istnieje w sieci
    """
    pass


class MethodNotFound(ServiceBusException):
    """
    Żądana funkcja nie istnieje w tym serwisie
    """
    pass

# async daeomn specific

class NotProcessedTask(Exception):
    """
    Task is not yet processed, no result is available.
    """
    pass
