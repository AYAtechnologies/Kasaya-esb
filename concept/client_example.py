#!/usr/bin/env python
#coding: utf-8
from client import sync, async


# wywołanie synchroniczne, anonimowe
sync.fikumiku.do_work("parameter", 1, foo=123, baz=True )

# dwa wywołania synchroniczne z prawami usera "roman"
with sync("roman") as S:
    S.fikumiku.do_work("parameter", 2, foo=456, baz=True )
    S.fikumiku.another_task("important parameter")

# wywołanie asynchroniczne,
# user o nazwie "stefan"
async("stefan").fikumiku.do_work("trololo", 3, foo=567, baz=False )

# wywołanie asynchroniczne, anonimowe
# rezultatem jest jakiś ID zadania
async.messages.mail.send_heavy_spam("ksiegowy@buziaczek.pl", howmany=5000 )


