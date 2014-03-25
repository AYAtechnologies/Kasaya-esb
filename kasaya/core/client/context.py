#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from kasaya.conf import settings
__all__ = ("Context",)


class Context(object):
    """
    Task context
    """
    def __init__(self, data=None):
        if data is None:
            self.__data = {
                'depth':settings.REQUEST_MAX_DEPTH,
            }
        elif type(data)==dict:
            self.__data = data

    # dictionary-like access
    def __setitem__(self, k,v):
        self.__data.__setitem__(k,v)
    def __getitem__(self, k):
        return self.__data.__getitem__(k)
    def __delitem__(self, k):
        self.__data.__delitem__(k)
    def __iter__(self):
        return self.__data.__iter__()
    def __len__(self):
        return self.__data.__len__()
    def __in__(self, k):
        return self.__data.__in__(k)
    def keys(self):
        return self.__data.keys()
    def items(self):
        return self.__data.items()
    def __repr__(self):
        return repr(self.__data)

    # data pickling
    def __getstate__(self):
        return self.__data
    def __setstate__(self, state):
        self.__data = state


    # authenticatin tokens
    def set_auth_token(self, token):
        """
        Setting authentication token in context
        """
        if 'token' in self:
            raise Exception("Context is already authenticated")
        self['token'] = token

    def get_auth_token(self):
        """
        Read auth token
        """
        try:
            return self['token']
        except KeyError:
            return None

    # calling task in current context
    @property
    def sync(self):
        try:
            return self.__sync
        except AttributeError:
            self.__sync = SyncExec(self)
        return self.__sync

    @property
    def async(self):
        try:
            return self.__async
        except AttributeError:
            self.__async = AsyncExec(self)
        return self.__async

    @property
    def trans(self):
        try:
            return self.__trans
        except AttributeError:
            self.__trans = TransactionExec(self)
        return self.__trans

    @property
    def control(self):
        try:
            return self.__control
        except AttributeError:
            self.__control = ControlExec(self)
        return self.__control



    # context manager
    #@classmethod
    #def __call__(cls, context):
    #    """
    #    To wywołanie używane jest przy tworzeniu context managera lub wywołaniu z określonym kontekstem wywołania.
    #    W takim przypadku należy utworzyć nową instancję tej klasy z ustawionym kontekstem.
    #    """
    #    return cls( context )

    def __enter__(self):
        """
        Ta metoda wywoływana jest przy wejściu do utworzonego context managera.
        """
        #print ("enter context", self.__data)
        return self

    def __exit__(self, typ, val, tback):
        """
        tutaj nic nie ma do zrobienia ponieważ nie przechwytujemy wyjątków
        """
        pass

# circular import
from .exec_context import SyncExec, AsyncExec, TransactionExec, ControlExec
