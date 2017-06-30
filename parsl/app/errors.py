class ParslError(Exception):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    pass


class NotFutureError(ParslError):
    ''' Basically a type error. A non future item was passed to a function
    that expected a future.
    '''
    pass

class InvalidAppTypeError(ParslError):
    ''' An invalid app type was requested from the the @App decorator.
    '''
    pass


class AppException(ParslError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    '''
    pass

class AppFailure(ParslError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    Contains:
    reason (string)
    exitcode (int)
    retries (int/None)
    '''

    def __init__(self, reason, exitcode, retries=None):
        self.reason = reason
        self.exitcode = exitcode
        self.retries = retries


class MissingOutputs(ParslError):
    ''' Error raised at the end of app execution due to missing
    output files

    Contains:
    reason (string)
    outputs (List of strings/files..)
    '''

    def __init__(self, reason, outputs):
        self.reason = reason
        self.outputs = outputs

    def __repr__ (self):
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)

    def __str__ (self):
        return "Reason:{0} Missing:{1}".format(self.reason, self.outputs)


