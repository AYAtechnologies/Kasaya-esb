#coding: utf-8
from __future__ import unicode_literals, division, absolute_import#, print_function



class kvstore(object):
    def __init__(self, db):
        self.db = db

    def __getitem__(self, k):
        return self.db._config_value_get(k)

    def __setitem__(self, k,v):
        self.db._config_value_set(k,v)

    def __delitem__(self, k):
        self.db._config_del(k)



class DatabaseBase(object):

    def __init__(self, workerid):
        self.ID = workerid
        # dictionary interface for config
        self.CONF = kvstore(self)

        # database ID
        self.dbid = self.CONF['databaseid']
        if self.dbid is None:
            import os, base64
            code = base64.b32encode( os.urandom(5) )
            self.dbid = code.decode("ascii").rstrip("=")
            self.CONF['databaseid'] = self.dbid

    def close(self):
        pass

    # config inteface

    def _config_value_get(self, key):
        pass

    def _config_value_set(self, key, val):
        pass

    def _config_del(self, key):
        pass
