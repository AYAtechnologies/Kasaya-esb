from uuid import uuid4


class AbstractStorage(object):
    def __init__(self, manager, **config):
        self.manager = manager
        self.init(**config)

    def begin(self):
        transaction = uuid4().hex
        self.initialize_transaction(transaction)
        return transaction

    def initialize_transaction(self, transaction_id):
        raise NotImplementedError

    def add_operation(self, transaction_id, operation, rollback):
        raise NotImplementedError

    def get(self, transaction_id):
        """This method should return transaction object"""
        raise NotImplementedError

    def remove_transaction(self, transaction_id):
        raise NotImplementedError

    def mark_transaction_succesfull(transaction_id, n):
        """This method must set the operation state as done"""
        raise NotImplementedError

    def mark_operation_failed(self, transaction_id, operation):
        raise NotImplementedError

    def mark_operation_success(self, transaction_id, operation):
        raise NotImplementedError
