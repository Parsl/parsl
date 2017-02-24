class DataFlowExceptions(Exception):
    """ Base class for all exceptions
    Only to be invoked when only a more specific error is not available.

    """
    pass

class DuplicateTaskError(DataFlowExceptions):
    pass

class MissingFutError(DataFlowExceptions):
    pass
