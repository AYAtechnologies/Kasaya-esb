#coding: utf-8
from __future__ import division, absolute_import, print_function, unicode_literals

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

# nazwy plików z socketami
#SOCK_QUERIES = "/tmp/esb_queries.sock"
#SOCK_LOCALWORKERS = "/tmp/esb_local_workers.sock"

# adres do jakiego zostaną podłączona sockety tcp
# dopuszczalne są:
#  nazwa interfejsu - "eth0", "eth1", ...
#  "AUTO" - podpięty do wszystkich interfejsów (alias dla 0.0.0.0)
#  "LOCAL" - alias dla 127.0.0.1
BIND_WORKER_TO = "eth0"

MAX_CONCURRENT_CONNECTIONS = 1000

# HEARBEAT

# co ile sekund wykonywany jest heartbeat
WORKER_HEARTBEAT = 5

# ile czasu ma syncd na odpowiedź
SYNC_REPLY_TIMEOUT = 2


# zakres portów do automatycznej alokacji dla workerów
WORKER_MIN_PORT = 5000
WORKER_MAX_PORT = 6000

SOCKET_BACKLOG = 50

# use monkey-patching from gevent on workers
WORKER_MONKEY = True

# konfiguracja name(?) serwera
# możliwe:
#  udp-broadcast, rijak,... cośtam cośtam
#SYNC_BACKEND = "udp-broadcast"

# jeśli serwer ma synchronizować się z innymi hostami poprzez rozsyłanie broadcastów,
# należy podać numer portu na którym nasłuchuje serwer
BROADCAST_PORT = 4040
# port na którym prowadzona jest synchronizacja pomiędzy serwerami syncd i zarządzanie siecią
KASAYAD_CONTROL_PORT = 4041
#SYNCD_CONTROL_BIND = "AUTO"

# stan sieci może być przechowywany w lokalnej bazie danych
KASAYAD_DB_BACKEND = "memory"

# nazwa workera odpowiedzialnego za wywołania asynchroniczne
ASYNC_DAEMON_SERVICE = "async_daemon"

#MIDDLEWARE_CLASSES = ["auth"]

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


# Select database backend for async daemon
# possible values: memory, redis
ASYNC_DAEMON_DB_BACKEND = "redis"



# builtin ESB workers
USE_ASYNC_SERVICE = False
USE_TRANSACTION_SERVICE = False
USE_AUTH_SERVICE = False
USE_LOG_SERVICE = False

# User workers directory
LOCAL_WORKERS_DIR = "/opt/services"
