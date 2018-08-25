"""Exceptions raise by Executors."""


class OptionalModuleMissing(Exception):
    ''' Error raised a required module is missing for a optional/extra provider
    '''

    def __init__(self, module_names, reason):
        self.module_names = module_names
        self.reason = reason

    def __repr__(self):
        return "Unable to initialize logger.Missing:{0},  Reason:{1}".format(
            self.module_names, self.reason
        )


class ExecutorError(Exception):
    """Base class for all exceptions.

    Only to be invoked when only a more specific error is not available.
    """

    def __init__(self, executor, reason):
        self.executor = executor
        self.reason = reason

    def __repr__(self):
        return "Executor {0} failed due to {1}".format(self.executor, self.reason)

    def __str__(self):
        return self.__repr__()


class ScalingFailed(ExecutorError):
    """Scaling failed due to error in Execution provider."""

    def __init__(self, executor, reason):
        self.executor = executor
        self.reason = reason


class ControllerError(ExecutorError):
    """Error raise by IPP controller."""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Controller init failed:Reason:{0}".format(self.reason)

    def __str__(self):
        return self.__repr__()
