#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
__all__ = ("Context",)


class Context(dict):
    """
    Task context
    """

    @classmethod
    def init_from_dict(cls, data):
        """
        Context creator using dict data as data source
        """
        ctx = cls()
        cls.update(data)
        return ctx


    def set_auth_token(self, token):
        """
        Setting authentication token in context
        """
        if '__token__' in self:
            raise Exception("Context is already authenticated")
        self['__token__'] = token

    def get_auth_token(self):
        """
        Read auth token
        """
        try:
            return self['__token__']
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
        print ("async")
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
        return self

    def __exit__(self, typ, val, tback):
        """
        tutaj nic nie ma do zrobienia ponieważ nie przechwytujemy wyjątków
        """
        pass

# circular import
from .exec_context import SyncExec, AsyncExec, TransactionExec, ControlExec
