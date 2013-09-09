__author__ = 'wektor'

from generic import GenericBackend
import redis


class RedisBackend(GenericBackend):

    def __init__(self):
         pool = redis.ConnectionPool(host='localhost', port=6379, db=0)
         self.store = redis.Redis(connection_pool=pool)

    def get_typecode(self, value):
        typecode = str(type(value)).split("'")[1]
        return typecode

    def set(self, key, value):
        data = {}
        data["type"] = self.get_typecode(value)
        data["data"] = value
        self.store.hmset(key, data)
    # def update(self, key, value):

    def get(self, key):
        data = self.store.hgetall(key)
        print data
        try:
            if data["type"] != "str":
                return eval(data["data"])
            else:
                return data["data"]
        except KeyError:
            return {}

    def delete(self, key):
        self.store.delete(key)