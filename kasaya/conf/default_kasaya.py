# coding: utf-8
#
# Common kasaya and kasayad default settings
#
from __future__ import division, absolute_import, print_function, unicode_literals

# GENERAL KASAYA ESB NETWORK SETTINGS

# whole service bus internally uses one protocol
# supported protocols: msgpack, bson
TRANSPORT_PROTOCOL = "pickle"

# internal setting, don't change
LOGGER_NAME = "kasaya"

# service bus name
SERVICE_BUS_NAME = "sb"

# użycie szyfrowania do wszelkiej transmisji poprzez ESB
ENCRYPTION = False
COMPRESSION = False

# zamiast plain password można wczytać zawartość jakiegoś tajemniczego pliku z /etc/esb/secret.txt
PASSWORD = ""


# worker's heartbeat to kasaya daemon
WORKER_HEARTBEAT = 5

# last heartbeat timeout
SYNC_REPLY_TIMEOUT = 2



# BINDING AND ADDRESS SETTINGS


# nazwy plików z socketami
#SOCK_QUERIES = "/tmp/esb_queries.sock"
#SOCK_LOCALWORKERS = "/tmp/esb_local_workers.sock"

# adres do jakiego zostaną podłączona sockety tcp
# dopuszczalne są:
#  nazwa interfejsu - "eth0", "eth1", ...
#  "AUTO" - podpięty do wszystkich interfejsów (alias dla 0.0.0.0)
#  "LOCAL" - alias dla 127.0.0.1
BIND_WORKER_TO = "0.0.0.0"

# how many concurrent connections is allowed in workers and kasaya daemon
MAX_CONCURRENT_CONNECTIONS = 1000

# zakres portów do automatycznej alokacji dla workerów
WORKER_MIN_PORT = 5000
WORKER_MAX_PORT = 6000
# don't change this if You don't know what it does
# (see unix socket documentation)
SOCKET_BACKLOG = 50

# jeśli serwer ma synchronizować się z innymi hostami poprzez rozsyłanie broadcastów,
# należy podać numer portu na którym nasłuchuje serwer
BROADCAST_PORT = 4040
# port na którym prowadzona jest synchronizacja pomiędzy serwerami syncd i zarządzanie siecią
KASAYAD_CONTROL_PORT = 4041




# GEVENT SETTINGS

# use monkey-patching from gevent on workers
WORKER_MONKEY = True


# KASAYA DAEMON INTERNAL SETTINGS

# currently only memory db backend is possible
# (it uses sqlite database internally)
KASAYAD_DB_BACKEND = "memory"


# LOGGING


# logowanie do pliku
LOG_TO_FILE = False
LOG_FILE_NAME = "/tmp/syncd.log"
# rotowanie plików logu
# LOG_ROTATE = 0

# poziom logowania
# DEBUG - 10, INFO - 20, WARNING - 30, ERROR - 40, CRITICAL - 50
# im niższy tym więcej informacji w logu
LOG_LEVEL = "DEBUG"

# jeśli True to standardowe wyjście zostanie również przekierowane do logu
# LOG_STDOUT = True


# WORKERS SETTINGS


# builtin ESB workers
USE_ASYNC_SERVICE = True
USE_TRANSACTION_SERVICE = False
USE_AUTH_SERVICE = False
USE_LOG_SERVICE = False

# User workers directory
LOCAL_WORKERS_DIR = "/opt/services"
