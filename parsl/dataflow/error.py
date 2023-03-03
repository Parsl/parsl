class DataFlowException(Exception):
    """Base class for all exceptions.

    Only to be invoked when only a more specific error is not available.

    """


class ConfigurationError(DataFlowException):
    """Raised when the DataFlowKernel receives an invalid configuration.
    """


class BadCheckpoint(DataFlowException):
    """Error raised at the end of app execution due to missing output files.

    Args:
         - reason

    Contains:
    reason (string)
    dependent_exceptions
    """

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return self.reason

    def __str__(self):
        return self.__repr__()


class DependencyError(DataFlowException):
    """Error raised if an app cannot run because there was an error
       in a dependency.

    Args:
         - dependent_exceptions_tids: List of dependency task IDs which failed
         - task_id: Task ID of the task that failed because of the dependency error
    """

    def __init__(self, dependent_exceptions_tids, task_id):
        self.dependent_exceptions_tids = dependent_exceptions_tids
        self.task_id = task_id

    def __str__(self):
        dep_tids = [tid for (exception, tid) in self.dependent_exceptions_tids]
        return "Dependency failure for task {} with failed dependencies from tasks {}".format(self.task_id, dep_tids)
