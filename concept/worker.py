#!/usr/bin/env python
#coding: utf-8
#from __future__ import unicode_literals

#import zerorpc
#
#c = zerorpc.Client()
#c.connect("tcp://127.0.0.1:6666")
#print c.hello("RPC")
import zmq
import json, random

class nscomm(object):
    def __init__(self):

        self.ctx = zmq.Context()
        self.ns_sender = self.ctx.socket(zmq.PUSH)
        self.ns_sender.connect('ipc://pingchannel')
        self.addr = "1.2.3.4:"+str(random.randint(5000,6000))

    def starting(self):
        msg = {"message":"connect"}
        msg['commchannel'] = self.addr
        msg['service'] = 'fikumiku'
        self.ns_sender.send( json.dumps(msg) )
        #self.ns_sender.send("ping")

    def stopping(self):
        msg = {"message":"disconnect"}
        msg['commchannel'] = self.addr
        self.ns_sender.send( json.dumps(msg) )


nsc = nscomm()
nsc.starting()
import time
time.sleep(60)
nsc.stopping()
