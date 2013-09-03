import os,sys
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../'))
from abstract import AbstractStorage
from transaction import Transaction


class Storage(AbstractStorage):
    def init(self, **kwargs):
        self.transactions = {}

    def initialize_transaction(self, transaction_id):
        self.transactions[transaction_id] = []

    def add_operation(self, transaction_id, operation, rollback):
        self.transactions[transaction_id].append(
            ({'method': operation}, {'method': rollback})
        )

    def get(self, transaction_id):
        t = Transaction()
        for op in self.transactions[transaction_id]:
            t.add_operation(op[0]['method'], op[1]['method'])

        return t

    def mark_operation_failed(self, transaction_id, operation):
        return self.mark_operation(transaction_id, operation, False, False)

    def mark_operation_success(self, transaction_id, operation):
        return self.mark_operation(transaction_id, operation, False, True)

    def mark_operation(self, transaction_id, operation, is_reverse=False, status=False):
        idx = 0
        if is_reverse:
            idx = 1
        self.transactions[transaction_id][operation][idx]['status'] = status
