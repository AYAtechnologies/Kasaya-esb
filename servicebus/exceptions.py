#coding: utf-8
from __future__ import unicode_literals


class ServiceBusException(Exception):
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


