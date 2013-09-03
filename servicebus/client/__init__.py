#coding: utf-8
from exec_context import ExecContext
from proxies import async_result

__all__ = ("sync", "async", "register_auth_processor", "async_result")

sync = ExecContext(default_proxy="sync")
async = ExecContext(default_proxy="async")
trans = ExecContext(default_proxy="trans")
control = ExecContext(default_proxy="control")

