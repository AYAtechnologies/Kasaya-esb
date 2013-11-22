#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
from __future__ import unicode_literals
from unittest import TestCase, main

from kasaya import conf
from kasaya import sync, async, trans, control

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
        with exc("something") as S:
            self.assertIsInstance( S, Exec )
            self.assertEqual( S._context, "something" )
            # method calling
            cll = S.foo.baz.bar
            self.assertEqual( ".".join(cll._names), "foo.baz.bar" )

        # same result as creating context is making new initialized Exec
        S = exc("foo")
        self.assertIsInstance( S, Exec )
        self.assertEqual( S._context, "foo" )

        # two initialized execution contexts are always different instances!
        S1 = exc("foo1")
        S2 = exc("foo2")
        self.assertEqual( S1._context, "foo1" )
        self.assertEqual( S2._context, "foo2" )
        self.assertNotEqual( id(S1), id(S2) )
        # calling methods
        cll = S1.foo.baz.bar
        self.assertEqual( ".".join(cll._names), "foo.baz.bar" )

        cll = exc.foo.baz.bar.bim
        self.assertEqual( ".".join(cll._names), "foo.baz.bar.bim" )


    def test_exec_context(self):
        self.subtest_exec_context(SyncExec, SyncProxy)
        self.subtest_exec_context(AsyncExec, AsyncProxy)
        self.subtest_exec_context(TransactionExec, TransactionProxy)
        self.subtest_exec_context(ControlExec, ControlProxy)



if __name__ == '__main__':
    main()
