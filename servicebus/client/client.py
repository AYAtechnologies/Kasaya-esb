#!/usr/bin/env python
#coding: utf-8
from servicebus import protocol
import zmq



class NSQuery(object):

    def __init__(self):
        self.ctx = zmq.Context()
        self.queries = self.ctx.socket(zmq.REQ)
        self.queries.connect('ipc://querychannel')

    def query(self, service):
        """
        odpytuje przez zeromq lokalny nameserver o to gdzie realizowany
        jest serwis o żądanej nazwie
        """
        msg = {'message':'query', 'service':service}
        self.queries.send(message_encode(msg))
        res = self.queries.recv()
        res = message_decode(res)
        return res


NS = NSQuery()


# wywołanie zdalnych funkcji


def sync_call(method, authinfo, args, kwargs):
    """
    Wywołanie synchroniczne jest wykonywane natychmiast.
    """
    print "SYNCHRONOUS",
    print "AUTHINFO:", authinfo
    print "method:", method
    worker = NS.query( method[0] )
#    print "Worker:",worker
    print "ARGS:",args,
    print "KWARGS:",kwargs
    print
#    return "fake result"


def async_call(method, authinfo, args, kwargs):
    """
    Wywołanie asynchroniczne powinno zostać zapisane w bazie i zostać wykonane
    w tle. Wynikiem funkcji powinien być identyfikator zadania wg którego można
    sprawdzić jego status.
    """
    print "ASYNCHRONOUS",
    print "AUTHINFO:", authinfo
    print "method:", method
    worker = NS.query( method[0] )
#    print "Worker:",worker
    print "ARGS:",args,
    print "KWARGS:",kwargs
    print
#    return "fake-id"





def auth_info_processor(authinfo):
    """
    Ta funkcja może np konwertować obiekt usera django na token, przydzielony mu podczas logowania
    który zostanie użyty przez workera do sprawdzenia czy user ma prawo do wykonania danej operacji.
    Albo cokolwiek innego.
    Sprawdzenie praw dostępu odbywać się powinno po stronie workera nie tutaj!
    """
    print "processing authinfo:", authinfo
    return authinfo




class FuncProxy(object):
    """
    Ta klasa robi magię i przekształca normalne pythonowe wywołanie z kropami:
    a.b.c.d
    na pojedyncze wywołanie z listą użytych metod ['a','b','c','d']
    """

    def __init__(self, top=None):
        self._top = top
        self._names = []
        self._method = None
        #self._user = None

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
        if m=="sync":
            return sync_call(
                self._top._names,
                self._top._authinfo,
                args,
                kwargs)
        elif m=="async":
            return async_call(
                self._top._names,
                self._top._authinfo,
                args,
                kwargs)
        else:
            raise Exception("Unknown call type")







class ExecAndContext(object):
    """
    Uruchamia zadania i tworzy context.
    """

    def __init__(self, authoinfo=None):
        self._authinfo = authoinfo

    def _make_proxy(self):
        proxy = FuncProxy()
        proxy._method = self.__class__.call_type
        proxy._top = proxy # << zrobić tutaj weakref aby zlikwidować cykliczne odwołanie do samego siebie
        proxy._authinfo = self._authinfo
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


