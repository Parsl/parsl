class DataFlowExceptions(Exception):
    """ Base class for all exceptions
    Only to be invoked when only a more specific error is not available.

    """
    pass

class DuplicateTaskError(DataFlowExceptions):
    """ Raised by the DataFlowKernel when it finds that a job with the same task-id has been
    launched before.
    """
    pass

class MissingFutError(DataFlowExceptions):
    """ Raised when a particular future is not found within the dataflowkernel's datastructures.
    Deprecated.
    """
    pass
