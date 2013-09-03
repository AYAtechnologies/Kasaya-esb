#encoding: utf-8
__author__ = 'wektor'

from proxies import *

class ExecContext(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, context=None, timeout=None, default_proxy="sync"):
        self._context = context
        self._timeout = timeout
        self._default_proxy = default_proxy
        self._proxy = {
            "sync": SyncProxy,
            "async": AsyncProxy,
            "control": ControlProxy,
            "trans": TransactionProxy
        }

    def _make_proxy(self, name):
        proxy = self._proxy[name]()
        print proxy
        proxy._top = proxy # << zrobić tutaj weakref aby zlikwidować cykliczne odwołanie do samego siebie
        proxy._context = self._context
        proxy._timeout = self._timeout
        return proxy

    def __getattribute__(self, itemname):
        if itemname.startswith("_"):
            return super(ExecContext, self).__getattribute__(itemname)
        if itemname in self._proxy.keys():
            print itemname
            proxy = self._make_proxy(itemname)
            #proxy._names.append(itemname)
        else:
            proxy = self._make_proxy(self._default_proxy)
            proxy._names.append(itemname)
        return proxy

    @classmethod
    def __call__(cls, context):
        """
        To wywołanie używane jest przy tworzeniu context managera lub wywołaniu z określonymi context.
        W takim przypadku należy utworzyć nową instancję tej klasy z ustawionym podanym parametrem authoinfo.

        Wynikiem jest nowa instancja własnej klasy z ustawionym context.
        """
        # global AUTHPROC
        # if not AUTHPROC is None:
        #     context = AUTHPROC(context)
        authexec = cls( context )
        return authexec

    def __enter__(self):
        """
        Ta metoda wywoływana jest przy wejściu do utworzonego context managera.
        W tym miejscu self reprezentuje obiekt authexec utworzony przez __call__.
        """
        return self

    def __exit__(self, typ, val, tback):
        """
        tutaj nic nie ma do zrobienia ponieważ nie przechwytujemy wyjątków
        """
        pass
