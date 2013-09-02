__author__ = 'wektor'

import inspect

class MiddlewareImplementationBase(object):
    pass

class MiddlewareCore(object):
    def __init__(self):
        #_middleware
        self.middleware_classes = []
        self._middleware = []

    def _biuld_middleware(self, mo):
        o = None
        if isinstance(mo, (list, tuple)):
            o = mo[0](self, *mo[1], **mo[2])
        else:
            if not o:
                o = mo(self)
            if not isinstance(o, MiddlewareImplementationBase):
                print "Unknown middleware call:", o
                return
        print o
        return o

    def setup_middleware(self):
        # this is hard and causes possible problems - will do it later
        # print sys.modules.keys()
        # #load _middleware from configuration
        # print settings.MIDDLEWARE
        # for m_module_name in settings.MIDDLEWARE:
        #     for sys_module_k in sys.modules.keys():
        #         if sys_module_k.endswith("_middleware."+m_module_name):
        #             print sys_module_k
        #             try:
        #                 worker_module = getattr(sys.modules[sys_module_k], "worker")
        #                 worker_class = worker_module.WorkerMiddleware
        #                 self.middleware_classes.append(worker_class)
        #             except AttributeError:
        #                 pass
        #initialize _middleware (from configuration and explicitly defined)
        for mp in self.middleware_classes:
            o = self._biuld_middleware(mp)
            if o:
                self._middleware.append(o)

    def load_middleware(self, name):
        """
        Load _middleware based on configuration
        """
        raise NotImplemented

    def prepare_message(self, message):
        """
        Run _middleware on message before executing
        """
        for m in self._middleware:
            mp = getattr(m, "prepare_message", None)
            if mp is not None:
                msg = mp(message)
                message = msg
        return message

    def postprocess_message(self, message):
        """
        Run _middleware on message after executing
        """
        for m in reversed(self._middleware):
            mp = getattr(m, "postprocess_message", None)
            if mp is not None:
                msg = mp(message)
                message = msg
        return message