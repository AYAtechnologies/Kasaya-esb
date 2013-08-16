#!/usr/bin/env python
#coding: utf-8
import sys,os
esbpath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append( esbpath )


from servicebus import client, conf
from servicebus.client import sync, async, register_auth_processor



if __name__=="__main__":
    conf.load_config_from_file("../config.txt")

    # wywołanie synchroniczne, anonimowe
    res = sync.fikumiku.do_work("parameter", 1, foo=123, baz=True )
    print "wynik fikumiku.do_work:" , repr(res)

    # dwa wywołania synchroniczne z prawami usera "roman"
    with sync("roman") as S:
        S.fikumiku.do_work("parameter", 2, foo=456, baz=True )
        S.fikumiku.another_task("important parameter")

    # wywołanie asynchroniczne,
    # user o nazwie "stefan"
    print async("stefan").fikumiku.do_work("trololo", 3, foo=567, baz=False )

    # wywołanie asynchroniczne, anonimowe
    # rezultatem jest jakiś ID zadania
    async.messages.mail.send_heavy_spam("ksiegowy@buziaczek.pl", howmany=5000 )

    sync.fikumiku.wyjebka(234)


