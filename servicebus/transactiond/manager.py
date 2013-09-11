import settings
from random import random


class TransactionManager(object):
    def __init__(self):
        self._storage = None

    @property
    def storage(self):
        """return transaction backend"""
        if not self._storage:
            self._storage = __import__(
                settings.STORAGE, fromlist=('Storage',)
            ).Storage(self, **settings.STORAGE_CONFIG)
        return self._storage

    def execute(self, operation):
        print("Wykonuje operacye %s" % operation)
        if operation == 'fikumiku' and random() > 0.5:
            raise Exception

    def begin(self):
        return self.storage.begin()

    def add_operation(self, transaction, operation, rollback):
        return self.storage.add_operation(transaction, operation, rollback)

    def commit(self, transaction_id):
        transaction = self.storage.get(transaction_id)
        for i in range(transaction.get_operations_count()):
            try:
                self.execute(transaction.get_operation(i))
                self.storage.mark_operation_success(transaction_id, i)
            except:
                from traceback import print_exc
                print_exc()
                self.storage.mark_operation_failed(transaction_id, i)
                for j in range(i):
                    self.execute(transaction.get_reverse_operation(j))
                break

    def rollback(self, transaction):
        return self.storage.rollback(transaction)

    def run(self):
        transaction = self.begin()
        print("New transaction %r" % transaction)
        for x in range(10):
            self.add_operation(transaction, 'fikumiku', 'mikufiku')
        self.commit(transaction)