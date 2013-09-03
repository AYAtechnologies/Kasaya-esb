import os,sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../'))

from uuid import uuid4
from abstract import AbstractStorage
import zmq
from transaction import Transaction
from pickle import dumps,loads


class Storage(AbstractStorage):
    def init(self, **kwargs):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(kwargs.pop('socket'))
        self.socket = socket
        self.send_to_storage(DictServer.CMD_PING)

    def initialize_transaction(self, transaction_id):
        self.send_to_storage(DictServer.CMD_INIT, transaction_id)

    def get(self, transaction_id):
        transaction = self.send_to_storage(DictServer.CMD_GET_TRANSACTION, transaction_id)
        if not transaction:
            raise ValueError
        t = Transaction()
        for i,operation in enumerate(transaction):
            t.add_operation(operation[0]['method'],operation[1]['method'])
        return t

    def add_operation(self, transaction_id, operation, rollback):
        self.send_to_storage(DictServer.CMD_ADD_OPERATION, transaction_id, operation, rollback)

    def mark_operation_failed(self, transaction_id, operation):
        return self.mark_operation(transaction_id, operation, False, False)

    def mark_operation_success(self, transaction_id, operation):
        return self.mark_operation(transaction_id, operation, False, True)

    def mark_operation(self, transaction_id, operation, is_reverse=False, status=False):
        self.send_to_storage(DictServer.CMD_SET_OPERATION_STATUS, transaction_id, operation, is_reverse, status)

    def send_to_storage(self, request, *args, **kwargs):
        params = dumps({
            'args': args,
            'kwargs': kwargs
        })

        msg = '%s:%s' % (request,params)
        self.socket.send(msg)
        recv = loads(self.socket.recv())
        print(request, " >= ", msg, "Received <= ", recv)
        if (recv == 'ERROR'):
            raise ValueError
        return recv


class DictServer(object):
    CMD_PING = 'PING'
    CMD_INIT = 'INITIALIZE'
    CMD_ADD_OPERATION = 'ADD'
    CMD_GET_TRANSACTION = 'GET'
    CMD_GET_OPERATION = 'GET_OP'
    CMD_GET_REV_OPERATION = 'GET_REV'
    CMD_GET_OPERATIONS_COUNT = 'GET_COUNT'
    CMD_SET_OPERATION_STATUS = 'SET_STATUS'

    def __init__(self, **kwargs):
        self.router = {
            self.CMD_INIT: self.initialize,
            self.CMD_PING: self.pong,
            self.CMD_ADD_OPERATION: self.add_to_transaction,
            self.CMD_GET_OPERATIONS_COUNT: self.get_operations_count,
            self.CMD_GET_TRANSACTION: self.get_transaction,
            self.CMD_GET_OPERATION: self.get_operation,
            self.CMD_GET_REV_OPERATION: self.get_reversal_operation,
            self.CMD_SET_OPERATION_STATUS: self.update_operation_status,
        }
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind(kwargs.pop('socket'))
        self.socket = socket
        self.transactions = {}

    def initialize(self, transaction_id):
        self.transactions[transaction_id] = []

    def pong(self):
        pass

    def get_transaction(self, transaction_id):
        if transaction_id not in self.transactions:
            return None
        return self.transactions[transaction_id]

    def add_to_transaction(self, transaction_id, operation, rollback):
        self.transactions[transaction_id].append(
            ({'method': operation}, {'method': rollback})
        )

    def get_operations_count(self, transaction_id):
        return len(self.transactions[transaction_id])

    def get_operation(self, transaction_id, operation):
        return self.transactions[transaction_id][operation][0]['method']

    def get_reversal_operation(self, transaction_id, operation):
        return self.transactions[transaction_id][operation][1]['method']

    def update_operation_status(self, transaction_id, operation, is_reverse, status):
        if is_reverse:
            self.transactions[transaction_id][operation][1]['status'] = status
        else:
            self.transactions[transaction_id][operation][0]['status'] = status

    def run(self):
        while True:
            message = self.socket.recv()
            request = message.split(':')
            try:
                method = request[0]
                params = loads(request[1])

                print('Received request', method, 'args=%r,kwargs=%r' % (params['args'],params['kwargs']))

                ret = dumps(
                    self.router[request[0]](
                        *params['args'],
                        **params['kwargs']
                    ) or 'OK'
                )
                self.socket.send(ret)
            except:
                from traceback import print_exc
                print_exc()
                self.socket.send(dumps("ERROR"))


if __name__ == '__main__':
    import settings
    server = DictServer(**settings.STORAGE_CONFIG)
    server.run()