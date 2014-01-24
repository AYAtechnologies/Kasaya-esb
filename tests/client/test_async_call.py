#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
from __future__ import unicode_literals
from unittest import TestCase, main
import types

from kasaya import conf
from kasaya import sync, async, trans, control, Context

from kasaya.core.client.exec_context import SyncExec, AsyncExec, TransactionExec, ControlExec
from kasaya.core.client.proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy


class TestExecutionCalls(TestCase):


    def subtest_exec_context(self, Exec, Proxy):
        # this is normally is done in client __init__.py
        exc = Exec()
        # called exec context should return self type initialized object
        self.assertIsInstance(exc, Exec)

        # subattrs ale Proxies
        f1 = exc.foo
        f2 = exc.foo.baz
        f3 = f2.baz.bar
        self.assertIsInstance(f1, Proxy)
        self.assertIsInstance(f2, Proxy)
        self.assertIsInstance(f3, Proxy)

        # separated calls from Exec object are always new instances
        self.assertNotEqual( id(f1), id(f2) )
        # but subcalls from previous attrs should return self's
        self.assertEqual( id(f2), id(f3) )

        # creating context creates Exec object with initialized _context attribute
        c = Context()
        c["aa"] = "bb"
        c2 = Context()
        c2["ee"] = "dd"

        with exc(c) as S:
            self.assertIsInstance( S, Exec )
            self.assertEqual( S._context, c )
            # method calling
            cll = S.foo.baz.bar
            self.assertEqual( ".".join(cll._names), "foo.baz.bar" )

        # same result as creating context is making new initialized Exec
        S = exc(c)
        self.assertIsInstance( S, Exec )
        self.assertEqual( S._context, c )

        # two initialized execution contexts are always different instances!
        S1 = exc(c)
        S2 = exc(c2)
        self.assertEqual( S1._context, c )
        self.assertEqual( S2._context, c2 )
        self.assertNotEqual( id(S1), id(S2) )
        # calling methods
        cll = S1.foo.baz.bar
        self.assertEqual( ".".join(cll._names), "foo.baz.bar" )

        cll = exc.foo.baz.bar.bim
        self.assertEqual( ".".join(cll._names), "foo.baz.bar.bim" )


        def own_send(self, addr, msg):
            print self
            print "addr", addr
            print "msg", msg

        def own_fw(self, sname, mname=None):
            return "tcp://127.0.0.1:5000"

        oldmethod1 = getattr(Proxy, "_send_and_response_message")
        setattr(Proxy, "_send_and_response_message", types.MethodType(own_send, Proxy))
        oldmethod2 = getattr(Proxy, "_find_worker")
        setattr(Proxy, "_find_worker", types.MethodType(own_fw, Proxy))

        ctx = Context()
        ctx['jajo']=10
        f = exc( ctx ).a.b.c
        print(f._names)

        # repair classes
        setattr(Proxy, "_send_and_response_message", oldmethod1)
        setattr(Proxy, "_find_worker", oldmethod2)



    def test_exec_context(self):
        #self.subtest_exec_context(SyncExec, SyncProxy)
        self.subtest_exec_context(AsyncExec, AsyncProxy)
        #self.subtest_exec_context(TransactionExec, TransactionProxy)
        #self.subtest_exec_context(ControlExec, ControlProxy)




if __name__ == '__main__':
    main()
