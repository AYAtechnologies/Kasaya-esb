#coding: utf-8
from servicebus.protocol import messages
from servicebus.conf import settings
from generic_proxy import GenericProxy
from servicebus.client.queries import SyncDQuery

class SyncProxy(GenericProxy):
    def __call__(self, *args, **kwargs):
        """
        Wywołanie synchroniczne jest wykonywane natychmiast.
        """
        method = self._names
        context = self._context
        addr = self._find_worker(method)
        # zbudowanie komunikatu
        msg = {
            "message" : messages.SYNC_CALL,
            "service" : method[0],
            "method" : method[1:],
            "context" : context,
            "args" : args,
            "kwargs" : kwargs
        }
        # wysłanie żądania
        #print "Sync task: ", addr, msg
        return self._send_message(addr, msg)


class AsyncProxy(GenericProxy):
    def __call__(self, *args, **kwargs):
        method = self._names
        context = self._context
        addr = self._find_worker([settings.ASYNC_DAEMON_SERVICE, "register_task"])
        # zbudowanie komunikatu
        msg = {
            "message" : messages.SYSTEM_CALL,
            "service" : settings.ASYNC_DAEMON_SERVICE,
            "method" : ["register_task"],
            "original_method": method,
            "context" : context,
            "args" : args,
            "kwargs" : kwargs
        }
        return self._send_message(addr, msg)

class ControlProxy(GenericProxy):
    def __call__(self, *args, **kwargs):
        method = self._names
        context = self._context
        msg = {
            "message" : messages.CTL_CALL,
            "method" : method,
            "context" : context,
            "args" : args,
            "kwargs" : kwargs
        }
        # wysłanie żądania
        #print "Control task: ", msg
        msgbody = SyncDQuery.control_task(msg)
        msg = msgbody['message']
        if msg==messages.RESULT:
            return msgbody['result']


class TransactionProxy(SyncProxy):
    pass
