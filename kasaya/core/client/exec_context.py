#encoding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals
from .proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy
from .context import Context


class ExecContext(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, context=None):
        if context is None:
            self._context = None
        elif isinstance(context, Context):
            self._context = context.copy()
        else:
            raise Exception("Context can be only Context type or None")

    def __getattr__(self, itemname):
        """
        When called immediatelly without context:
            sync.service.function()
        we must create proxy instance and return it back as result
        """
        proxy = self._create_proxy()
        proxy._context = self._context
        proxy._names.append(itemname)
        return proxy

    @classmethod
    def __call__(cls, context):
        """
        To wywołanie używane jest przy tworzeniu context managera lub wywołaniu z określonym contekstem wywołania.
        W takim przypadku należy utworzyć nową instancję tej klasy z ustawionym kontekstem.
        """
        return cls( context )

    def __enter__(self):
        """
        Ta metoda wywoływana jest przy wejściu do utworzonego context managera.
        W tym miejscu self reprezentuje obiekt typu ExecContext utworzony przez __call__.
        """
        return self

    def __exit__(self, typ, val, tback):
        """
        tutaj nic nie ma do zrobienia ponieważ nie przechwytujemy wyjątków
        """
        pass



class SyncExec(ExecContext):
    def _create_proxy(self):
        return SyncProxy()

class AsyncExec(ExecContext):
    def _create_proxy(self):
        return AsyncProxy()

class TransactionExec(ExecContext):
    def _create_proxy(self):
        return TransactionProxy()

class ControlExec(ExecContext):
    def _create_proxy(self):
        return ControlProxy()
