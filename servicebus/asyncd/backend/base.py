__author__ = 'wektor'


class TaskNotFound(Exception):
    pass



class BackendBase(object):
    """
    Class which is the interface for all queue backends.
    It lists all methods which need to be implemented.
    """

    def add_task(self, task):
        return task_id

    def del_task(self, task):
        return True

    def get_result(self, task_id):
        return result

    def set_result_success(self, task_id, result):
        pass

    def set_result_fail(self, task_id, error_code, error):
        pass