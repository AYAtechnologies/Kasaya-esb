__author__ = 'wektor'

import riak
from generic import GenericBackend

class RiakBackend(GenericBackend):

    def __init__(self, bucket = "test"):
        client = riak.RiakClient(pb_port=8087, protocol='pbc')
        self.store = client.bucket(bucket)

    def set(self, key, value):
        k = self.store.new(key, value)
        k.store()

    def get(self, key):
        val = self.store.get(key)
        return val.data

    def delete(self, key):
        val = self.store.get(key)
        val.delete()
