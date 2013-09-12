class Transaction(object):
    def __init__(self, operations=None):
        self.operations = []
        self.reverse_operations = []

        if operations:
            for op in operations:
                self.add_operation(op[0], op[1])

    def add_operation(self, operation, reverse):
        self.operations.append({
            'method': operation,
            'status': None
        })
        self.reverse_operations.append({
            'method': reverse,
            'status': None
        })

    def get_reverse_operation(self, i):
        return self.reverse_operations[i]['method']

    def get_operation(self, i):
        return self.operations[i]['method']

    def get_operations_count(self):
        return len(self.operations)
