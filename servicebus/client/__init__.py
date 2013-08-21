#coding: utf-8
from funcproxy import SyncExec, AsyncExec, register_auth_processor
from task_caller import get_async_result

__all__ = ("sync", "async", "register_auth_processor", "get_async_result")

sync = SyncExec()
async = AsyncExec()


