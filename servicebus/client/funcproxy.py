#!/usr/bin/env python
#coding: utf-8
from .task_caller import execute_sync_task, register_async_task



class FuncProxy(object):
    """
    Ta klasa przekształca normalne pythonowe wywołanie z kropkami:
    a.b.c.d
    na pojedyncze wywołanie z listą użytych metod ['a','b','c','d']
    """

    def __init__(self, top=None):
        self._top = top
        self._names = []
        self._method = None

    def __getattribute__(self, itemname):
        if itemname.startswith("_"):
            return super(FuncProxy, self).__getattribute__(itemname)
        M = FuncProxy(top=self._top)
        self._top._names.append(itemname)
        return M

    def __contains__(self, name):
        """
        Wszystkie atrybuty są możliwe, bo każdy atrybut może istnieć
        po stronie workera, dlatego zawsze True. W sumie nie wiem czy to
        jest potrzebna metoda, ale niech na razie będzie.
        """
        return True

    def __call__(self, *args, **kwargs):
        m = self._top._method
        top = self._top
        del self._top   # pomagamy garbage collectorowi
        if m=="sync":
            return execute_sync_task(
                top._names,
                top._authinfo,
                top._timeout,
                args,
                kwargs)
        elif m=="async":
            return register_async_task(
                top._names,
                top._authinfo,
                top._timeout,
                args,
                kwargs)
        else:
            raise Exception("Unknown call type")




class ExecAndContext(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, authoinfo=None, timeout=None):
        self._authinfo = authoinfo
        self._timeout = timeout

    def _make_proxy(self):
        proxy = FuncProxy()
        proxy._method = self.__class__.call_type
        proxy._top = proxy # << zrobić tutaj weakref aby zlikwidować cykliczne odwołanie do samego siebie
        proxy._authinfo = self._authinfo
        proxy._timeout = self._timeout
        return proxy

    def __getattribute__(self, itemname):
        if itemname.startswith("_"):
            return super(ExecAndContext, self).__getattribute__(itemname)
        proxy = self._make_proxy()
        proxy._names.append(itemname)
        return proxy

    @classmethod
    def __call__(cls, authinfo):
        """
        To wywołanie używane jest przy tworzeniu context managera lub wywołaniu z określonymi authinfo.
        W takim przypadku należy utworzyć nową instancję tej klasy z ustawionym podanym parametrem authoinfo.

        Wynikiem jest nowa instancja własnej klasy z ustawionym authinfo.
        """
        authexec = cls( auth_info_processor(authinfo) )
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



class SyncExec(ExecAndContext):
    """
    Wywołania synchroniczne
    """
    call_type = "sync"



class AsyncExec(ExecAndContext):
    """
    Wywołania asynchroniczne
    """
    call_type = "async"


