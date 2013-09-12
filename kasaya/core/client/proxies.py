#coding: utf-8
from kasaya.core.protocol import messages
from kasaya.core.client.queries import SyncDQuery
from kasaya.conf import settings
from generic_proxy import GenericProxy

class SyncProxy(GenericProxy):
    def __call__(self, *args, **kwargs):
        """
        Wywołanie synchroniczne jest wykonywane natychmiast.
        """
        method = self._names
        context = self._context
        addr = self.find_worker(method)
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
        addr = self.find_worker([settings.ASYNC_DAEMON_SERVICE, "register_task"])
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

def async_result(task_id, context):
    #execute_sync_task(method, context, args, kwargs, addr = None)
    m = [settings.ASYNC_DAEMON_SERVICE, "get_task_result"]
    s = SyncProxy()
    s._names = m
    s._context = context
    return s(task_id)

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
