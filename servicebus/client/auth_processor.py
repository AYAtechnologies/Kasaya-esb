#!/usr/bin/env python
#coding: utf-8



def auth_info_processor(authinfo):
    """
    Ta funkcja może np konwertować obiekt usera django na token, przydzielony mu podczas logowania
    który zostanie użyty przez workera do sprawdzenia czy user ma prawo do wykonania danej operacji.
    Albo cokolwiek innego.
    Sprawdzenie praw dostępu odbywać się powinno po stronie workera nie tutaj!
    """
    print "processing authinfo:", authinfo
    return authinfo

