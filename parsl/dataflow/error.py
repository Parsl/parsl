class DataFlowException(Exception):
    """Base class for all exceptions.

    Only to be invoked when only a more specific error is not available.

    """


class ConfigurationError(DataFlowException):
    """Raised when the DataFlowKernel receives an invalid configuration.
    """


class DuplicateTaskError(DataFlowException):
    """Raised by the DataFlowKernel when it finds that a job with the same task-id has been launched before.
    """


class MissingFutError(DataFlowException):
    """Raised when a particular future is not found within the dataflowkernel's datastructures.

    Deprecated.
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
    """Error raised at the end of app execution due to missing output files.

    Args:
         - dependent_exceptions: List of exceptions
         - task_id: Identity of the task failed task
         - outputs ?

    Contains:
    reason (string)
    dependent_exceptions
    """

    def __init__(self, dependent_exceptions, task_id, outputs):
        self.dependent_exceptions = dependent_exceptions
        self.task_id = task_id
        self.outputs = outputs

    def __repr__(self):
        return "[{}] Dependency failure from: {}".format(self.task_id,
                                                         self.dependent_exceptions)

    def __str__(self):
        return self.__repr__()
