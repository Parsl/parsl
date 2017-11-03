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

class DependencyError(DataFlowExceptions):
    ''' Error raised at the end of app execution due to missing
    output files

    Contains:
    reason (string)
    outputs (List of strings/files..)
    '''

    def __init__(self, dependent_exceptions, reason, outputs):
        self.dependent_exceptions = dependent_exceptions
        self.reason = reason
        self.outputs = outputs

    def __repr__ (self):
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)

    def __str__ (self):
        return "Reason:{0} Missing:{1}".format(self.reason, self.outputs)


class ControllerErr(DataFlowExceptions):
    ''' Error raise by IPP controller
    '''
    def __init__(self, reason):
        self.reason = reason

    def __repr__ (self):
        return "Controller init failed:Reason:{0}".format(self.reason)

    def __str__ (self):
        return self.__repr__()


