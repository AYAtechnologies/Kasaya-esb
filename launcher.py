#!/usr/bin/env python
#coding: utf-8

#from manager import TransactionManager

#if __name__ == '__main__':
#    tm = TransactionManager()
#    tm.run()

from protocol import *
import datetime

d1 = datetime.datetime.now()
d2 = datetime.date.today()
d3 = d1.time()
d4 = datetime.timedelta(days=12, seconds=3472, weeks=23, microseconds=358345)

d = {'foo':123, 'baz':[1,2,3.45,4,5], 'bar':(123,'abc',d1, d2, d3, d4)}
m = serialize(d)
r = deserialize(m)

import pprint
pprint.pprint (r)
