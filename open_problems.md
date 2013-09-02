
# Cleaning of the async task queue
    - who and when should do it
        - maybe at the start -> if one process was dead NS should detect it and kill( or at leas flag as dead) then a new process should be started and it can clean after the dead one
    - saving process id (to check if no process is hanging)
    - timeout of async tasks (global/local)

# API sugar for getting async results

# Backends
    - Redis backend (for dev)
    - Riak backend (for prod)

# Middleware
    ## Filter middleware
    eg. auth
    - all messages go thru all middleware and each middleware might drop task

    ## Extend middleware
    eg. Logging, Promotions
    - all tasks go thru midleware and the middleware might change the content / extend it or just log the fact of the message

# Events
    - Method of handling events - asynchronous messages
        - callback ?
            - event decorator - new event handler deamon should be built.

        - subscribing to events ?
            - using regexp of methods?

# Tests
    - trzeba wymyslic wszystkie mozliwe sytuacje i to przetestowac w szczegolnosci sync serwer
     - pad procesu (kazdego typu) w kazdej chwili (w trakcie odbierania wiadomosci, w trakcie przetwarzania, w trakcie odpowiedzi)
     - pad lacza w kazdej chwili
    pad czegokolwiek powinien zwrocic blad wszystkim a nie moze to przejsc dalej

    WALIDACJA ASYNC TASKOW:
    - async zleca task do workera i async pada:
        - wynik wraca zanim async wstanie
            - worker musi miec timeout
            - wynik jest tracony
        - wynik wraca jak nowy async wstanie
            - co sie dzieje? nowy dostanie wynik czy bedzie jakis fail?

    - async pada zanim task wyjdzie do workera (dluga kolejla)
        - nowy async dodaje te taski do swojej kolejki


# Globalna konfiguracja utrzymywana przez synncd
    - API do czytania i zmiany tej konfiguracji