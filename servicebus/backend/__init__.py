#coding: utf-8
__author__ = 'wektor'

def get_backend_class(name):
    if name=="memory":
        from servicebus.asyncd.backend.mem import MemoryAsyncBackend
        return MemoryAsyncBackend
    elif name=="redis":
        from redisstore import RedisBackend
        return RedisBackend
    raise Exception("Unknown backend [%s]" % str(name) )
