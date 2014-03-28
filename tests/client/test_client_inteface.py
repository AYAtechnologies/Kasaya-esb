#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
from __future__ import unicode_literals
from unittest import TestCase, main
#import types
#from kasaya import conf
from kasaya import sync, async, trans, control, Context


_backup_methods={}
def replace_method(cls, method_name, func):
    """
    Replaces method of class by own function and remember original method for use in function restore_method
    """
    import types
    global _backup_methods
    mth = getattr(cls, method_name)
    if not cls.__name__ in _backup_methods:
        _backup_methods[cls.__name__] = {}
    _backup_methods[cls.__name__][method_name] = mth
    setattr(cls, method_name, types.MethodType(func, cls))

def restore_method(cls, method_name):
    """
    Restores back original method of class replaced by replace_method function
    """
    global _backup_methods
    mth = _backup_methods[cls.__name__][method_name]
    setattr(cls, method_name, mth)


class TestExecutionCalls(TestCase):


    def setUp(self):
        # fake methods
        self.msg_callback = None
        def fake_find_worker(self, sname, mname=None):
            """ fake _find_worker method """
            return "tcp://127.0.0.1:5000"

        def fake_send_and_response_message(self, addr, msg):
            """ fake _send_and_response_message method """
            global msg_callback
            if msg_callback is None:
                return "fake_result"
            return msg_callback(msg)

        from kasaya.core.client.proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy
        replace_method(SyncProxy, "_find_worker", fake_find_worker)
        replace_method(SyncProxy, "_send_and_response_message", fake_send_and_response_message)
        #replace_method(AsyncProxy, "_find_worker", fake_find_worker)
        #replace_method(AsyncProxy, "_send_and_response_message", fake_send_and_response_message)


    def tearDown(self):
        from kasaya.core.client.proxies import SyncProxy, AsyncProxy, ControlProxy, TransactionProxy
        # restore original methods
        restore_method(SyncProxy, "_find_worker")
        restore_method(SyncProxy, "_send_and_response_message")
        #restore_method(AsyncProxy, "_find_worker")
        #restore_method(AsyncProxy, "_send_and_response_message")

    def test_anonymous_calls(self):
        global msg_callback
        def context_should_be_empty(msg):
            self.assertEqual( len(msg['context']), 1 )
            self.assertIn( 'depth', msg['context'] )
        msg_callback = context_should_be_empty

        sync.aa()
        sync.aa.bb()

        ctx = Context()
        def context_should_be_ctx(msg):
            # check if context was transferred
            c = msg['context']
            self.assertEqual(c['foo'],'test1')
            self.assertEqual(c.get_auth_token(), "token")
        msg_callback = context_should_be_ctx

        ctx['foo'] = "test1"
        with ctx as C:
            C.set_auth_token("token")
            C.sync.aaa.bbb()

            #C.async.aaa.bbb()
            #C.control.aaa.bbb()
            C.trans.aaa.bbb()


class TestContext(TestCase):

    def test_context(self):
        ctx = Context()
        ctx['a'] = 1
        ctx['b'] = 2
        del ctx['a']



if __name__ == '__main__':
    main()
