#coding: utf-8
from funcproxy import SyncExec, AsyncExec, ControlExec, register_auth_processor
from task_caller import async_result

__all__ = ("sync", "async", "register_auth_processor", "async_result")

sync = SyncExec()
async = AsyncExec()
busctl = ControlExec()
