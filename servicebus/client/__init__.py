#coding: utf-8
from funcproxy import SyncExec, AsyncExec, register_auth_processor

__all__ = ("sync", "async", "register_auth_processor")

sync = SyncExec()
async = AsyncExec()


