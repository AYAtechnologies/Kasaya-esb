STORAGE = 'storage.dict'
STORAGE_CONFIG = {
    'host': 'localhost',
    'socket': 'ipc://dict-storage',
}

try:
    from local_settings import *
except ImportError:
    pass