#!/home/moozg/venvs/kasatest/bin/python
#coding: utf-8
#!/home/moozg/venvs/kasa33/bin/python
from __future__ import division, absolute_import, print_function, unicode_literals
import unittest, os
# misc
from kasaya.conf import set_value, settings
from kasaya.core.lib import comm
from gevent import socket
import gevent, random


class AddrDecoderTest(unittest.TestCase):

    def test_decode_addr(self):
        res = comm.decode_addr("tcp://127.0.0.1:123")
        self.assertItemsEqual( res,  ('tcp',('127.0.0.1',123), socket.AF_INET, socket.SOCK_STREAM) )
        res = comm.decode_addr("ipc:///tmp/my_socket.sock")
        self.assertItemsEqual( res,  ('ipc',"/tmp/my_socket.sock", socket.AF_UNIX, socket.SOCK_STREAM) )


class MaliciousSender(comm.Sender):
    """
    Special sender which is able to send broken messages
    """
    def send_raw(self, rawdata):
        self.SOCK.sendall( rawdata )



def _setup_connecion():
    global MLOOP, SENDER, grlt
    addr = "tcp://127.0.0.1:56780"
    # message loop
    MLOOP = comm.MessageLoop(addr)
    grlt = gevent.spawn(MLOOP.loop) # spawn listener
    # sender
    SENDER = MaliciousSender(addr)

def _cleanup_connection():
    global MLOOP, SENDER
    MLOOP.kill()
    MLOOP.close()
    SENDER.close()



class SocketServerTest(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        _setup_connecion()

    @classmethod
    def tearDownClass(self):
        _cleanup_connection()

    def random_key(self, length=8):
        n = ""
        for i in range(length):
            n+=random.choice("0123456789abcdefgihjklmnopqrstuvwxyz")
        return n

    def random_val(self):
        t = random.choice("fis")
        if t=="f":
            return random.random()*1000
        elif t=="i":
            return random.randint(-10000,10000)
        elif t=="s":
            return self.random_key(80)


    def test_simple_transmission(self):
        global MLOOP, SENDER
        self.test_size = 0
        self.pattern = None
        def response(msg):
            # there will be error raised if data don't arrive
            self.assertItemsEqual(self.pattern, msg)
            self.test_size -= 1

        # spawn listener
        MLOOP.register_message("test", response)
        # send bunch of messages
        for msg in range(10):
            self.test_size += 1
            msg = {"message":"test"}
            for n in range(8):
                msg[self.random_key()]=self.random_val()
            # send it
            self.pattern=msg
            SENDER.send(msg)
            # wait until receiver get incoming message
            while self.test_size>0:
                gevent.sleep(0.01)

        # cleanup
        del self.test_size
        del self.pattern


    def test_broken_and_proper_transmission(self):
        global MLOOP, SENDER
        self.test_size = 0
        self.pattern = None

        def response(msg):
            # there will be error raised if data don't arrive
            self.assertItemsEqual(self.pattern, msg)
            self.test_size -= 1

        # spawn listener
        MLOOP.register_message("test", response)

        self.test_size += 1
        msg = {"message":"test"}

        # send it
        self.pattern=msg
        SENDER.send(msg)
        # wait until receiver get incoming message
        while self.test_size>0:
            gevent.sleep(0.01)

        # cleanup
        del self.test_size
        del self.pattern


if __name__ == '__main__':
    unittest.main()
