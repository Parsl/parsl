''' Exceptions raise by Apps.
'''
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
    def __repr__(self):
        return "{0} Reason:{1}".format(self.__class__, self.reason)

    def __str__(self):
        return self.__repr__()

class AppBadFormatting(ParslError):
    ''' An error raised during formatting of a bash function
    What this exception contains depends entirely on context
    Contains:
    reason(string)
    exitcode(int)
    retries(int/None)
    '''

    def __init__(self, reason, exitcode, retries=None):
        super().__init__()
        self.reason = reason
        self.exitcode = exitcode
        self.retries = retries


class AppFailure(AppException):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    Contains:
    reason(string)
    exitcode(int)
    retries(int/None)
    '''

    def __init__(self, reason, exitcode, retries=None):
        super().__init__()
        self.reason = reason
        self.exitcode = exitcode
        self.retries = retries

class AppTimeout(AppException):
    ''' An error raised during execution of an app when it exceeds its allotted walltime

    Contains:
    reason(string)
    exitcode(int)
    retries(int/None)
    '''

    def __init__(self, reason, exitcode, retries=None):
        super().__init__()
        self.reason = reason
        self.exitcode = -55
        self.retries = retries


class MissingOutputs(ParslError):
    ''' Error raised at the end of app execution due to missing
    output files

    Contains:
    reason(string)
    outputs(List of strings/files..)
    '''

    def __init__(self, reason, outputs):
        super().__init__()
        self.reason = reason
        self.outputs = outputs

    def __repr__(self):
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)

    def __str__(self):
        return "Reason:{0} Missing:{1}".format(self.reason, self.outputs)

class DependencyError(ParslError):
    ''' Error raised at the end of app execution due to missing
    output files

    Contains:
    reason(string)
    outputs(List of strings/files..)
    '''

    def __init__(self, dependent_exceptions, reason, outputs):
        super().__init__()
        self.dependent_exceptions = dependent_exceptions
        self.reason = reason
        self.outputs = outputs

    def __repr__(self):
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)

    def __str__(self):
        return "Reason:{0} Missing:{1}".format(self.reason, self.outputs)
