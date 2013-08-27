#coding: utf-8
from __future__ import unicode_literals

# użycie szyfrowania do wszelkiej transmisji poprzez ESB
ENCRYPTION = False
COMPRESSION = False

# zamiast plain password można wczytać zawartość jakiegoś tajmeniczego pliku z /etc/esb/secret.txt
PASSWORD = ""

# nazwy plików z socketami
SOCK_QUERIES = "/tmp/esb_queries.sock"
#SOCK_LOCALWORKERS = "/tmp/esb_local_workers.sock"

# adres do jakiego zostaną podłączona sockety tcp
# dopuszczalne są:
#  nazwa interfejsu - "eth0", "eth1", ...
#  "LOCAL" - podpięty do lokalnego IP (127.0.0.1)
#  "AUTO" - podpięty do pierwszego interfejsu nie-lokalnego (zwykle eth0)
BIND_WORKER_TO = "AUTO"

WORKER_POOL_SIZE = 10

# HEARBEAT

# co ile sekund wykonywany jest cykl heartbeat
WORKER_HEARTBEAT = 5
# czas oczekiwania na odpowiedź od workera po wysłaniu pinga do niego
PING_TIMEOUT = 3
# czas przez jaki worke traktowany jest jako działający
# od ostatniej aktywności lub odpowiedzi na ping
# (powinien być większy niż WORKER_HEARTBEAT )
HEARTBEAT_TIMEOUT = 10


# zakres portów do automatycznej alokacji dla workerów
WORKER_MIN_PORT = 5000
WORKER_MAX_PORT = 6000

# konfiguracja name(?) serwera
# możliwe:
#  UDPBROADCAST, RIJAKDB,... cośtam cośtam
SYNC_BACKEND = "udp-broadcast"

# jeśli serwer ma synchronizować się z innymi hostami poprzez rozsyłanie broadcastów,
# należy podać numer portu na którym nasłuchuje serwer
BROADCAST_PORT = 4040
# port na którym prowadzona jest synchronizacja pomiędzy serwerami syncd i zarządzanie siecią
SYNCD_CONTROL_PORT = 4041
SYNCD_CONTROL_BIND = "AUTO"

# stan sieci może być przechowywany w lokalnej bazie danych, jeśli
## przemyśleć to
DB_BACKEND = "dict"

# nazwa workera odpowiedzialnego za wywołania asynchroniczne
ASYNC_DAEMON_SERVICE = "async_daemon"

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

