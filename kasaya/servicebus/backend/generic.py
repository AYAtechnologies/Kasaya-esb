__author__ = 'wektor'


class GenericBackend(object):

    def set(self, key, value):
        raise NotImplemented

    def get(self, key):
        raise NotImplemented

    def delete(self, key):
        raise NotImplemented