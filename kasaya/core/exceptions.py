#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals


class ServiceBusException(Exception):
    pass


class NotOurMessage(ServiceBusException):
    pass

class ReponseTimeout(ServiceBusException):
    pass

class MessageCorrupted(ServiceBusException):
    """
    Nadchodzączy komunikat jest uszkodzony, lub rozszyfrowanie się nie powiodło.
    """
    pass


class ExecutionRejected(ServiceBusException):
    """
    Wykonanie metody odrzucone z powodu braku uprawnień.
    """
    pass

class AnonymousExecutionDisabled(ExecutionRejected):
    """
    Wykonanie metody anonimowo jest zabronione
    """
    pass

class NotEnoughPriviliges(ExecutionRejected):
    """
    Niewystarczajace uprawnienia do wykonania metody
    """
    pass


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