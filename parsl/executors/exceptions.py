class ExecutorException(Exception):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    pass


class TaskExecException(ExecutorException):
    """ Task execution raised an error in the remote process
    """
    pass
