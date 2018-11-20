"""Exceptions raise by Apps."""
from functools import wraps

import dill
from six import reraise


class ParslError(Exception):
    """Base class for all exceptions.

    Only to be invoked when a more specific error is not available.
    """


class NotFutureError(ParslError):
    """A non future item was passed to a function that expected a future.

    This is basically a type error.
    """


class InvalidAppTypeError(ParslError):
    """An invalid app type was requested from the @App decorator."""


class AppException(ParslError):
    """An error raised during execution of an app.

    What this exception contains depends entirely on context
    """


class AppBadFormatting(ParslError):
    """An error raised during formatting of a bash function.
    """


class AppFailure(AppException):
    """An error raised during execution of an app.

    What this exception contains depends entirely on context
    Contains:
    reason(string)
    exitcode(int)
    retries(int/None)
    """

    def __init__(self, reason, exitcode, retries=None):
        self.reason = reason
        self.exitcode = exitcode
        self.retries = retries


class AppTimeout(AppException):
    """An error raised during execution of an app when it exceeds its allotted walltime.
    """


class BashAppNoReturn(AppException):
    """Bash app returned no string.

    Contains:
    reason(string)
    exitcode(int)
    retries(int/None)
    """

    def __init__(self, reason, exitcode=-21, retries=None):
        super().__init__(reason)
        self.reason = reason
        self.exitcode = exitcode
        self.retries = retries


class MissingOutputs(ParslError):
    """Error raised at the end of app execution due to missing output files.

    Contains:
    reason(string)
    outputs(List of strings/files..)
    """

    def __init__(self, reason, outputs):
        super().__init__(reason)
        self.reason = reason
        self.outputs = outputs

    def __repr__(self):
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)

    def __str__(self):
        return "Reason:{0} Missing:{1}".format(self.reason, self.outputs)


class BadStdStreamFile(ParslError):
    """Error raised due to bad filepaths specified for STDOUT/ STDERR.

    Contains:
       reason(string)
       outputs(List of strings/files..)
       exception object
    """

    def __init__(self, outputs, exception):
        super().__init__()
        self._outputs = outputs
        self._exception = exception

    def __repr__(self):
        return "FilePath:[{}] Exception:{}".format(self._outputs,
                                                   self._exception)

    def __str__(self):
        return self.__repr__()


class DependencyError(ParslError):
    """Error raised at the end of app execution due to missing output files.

    Contains:
    reason(string)
    outputs(List of strings/files..)
    """

    def __init__(self, dependent_exceptions, reason, outputs):
        super().__init__(reason)
        self.dependent_exceptions = dependent_exceptions
        self.reason = reason
        self.outputs = outputs

    def __repr__(self):
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)

    def __str__(self):
        return "Reason:{0} Missing:{1}".format(self.reason, self.outputs)


# removed RemoteException from being an actual exception so
# that we might have some better chance at serialising it.
class RemoteException:
    def __init__(self, e_type, e_value, traceback):
        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)

        # what can we do with the traceback? I'd like it to appear
        # remotely? perhaps at the point that the RemoteException is
        # re-raised we could output it somehow? 
        # or maybe we don't try to pass back exactly the same exception
        # class?

        # or look at tblib... which is already there but somehow not working
        # as might have been intended?
        # self.traceback = dill.dumps(traceback)

    def reraise(self):
        reraise(dill.loads(self.e_type), dill.loads(self.e_value))


def wrap_error(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        import sys
        from parsl.app.errors import RemoteException
        try:
            return func(*args, **kwargs)
        except Exception:
            return RemoteException(*sys.exc_info())
    return wrapper
