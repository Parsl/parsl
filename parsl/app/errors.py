"""Exceptions raised by Apps."""
from functools import wraps
from typing import List, Union, Callable, Dict, Tuple, Any
from types import TracebackType

import dill
import logging
from tblib import Traceback

from six import reraise

from parsl import File

logger = logging.getLogger(__name__)


class ParslError(Exception):
    """Base class for all exceptions.

    Only to be invoked when a more specific error is not available.
    """


class NotFutureError(ParslError):
    """A non future item was passed to a function that expected a future.

    This is basically a type error.
    """


class AppException(ParslError):
    """An error raised during execution of an app.

    What this exception contains depends entirely on context
    """


class AppBadFormatting(ParslError):
    """An error raised during formatting of a bash function.
    """


class BashExitFailure(AppException):
    """A non-zero exit code returned from a @bash_app

    Contains:
    reason(str)
    exitcode(int)
    """

    def __init__(self, reason: str, exitcode: int):
        self.reason = reason
        self.exitcode = exitcode


class AppTimeout(AppException):
    """An error raised during execution of an app when it exceeds its allotted walltime.
    """


class BashAppNoReturn(AppException):
    """Bash app returned no string.

    Contains:
    reason(string)
    """

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class MissingOutputs(ParslError):
    """Error raised at the end of app execution due to missing output files.

    Contains:
    reason(string)
    outputs(List of strings/files..)
    """

    def __init__(self, reason: str, outputs: List[Union[str, File]]):
        super().__init__(reason, outputs)
        self.reason = reason
        self.outputs = outputs

    def __repr__(self) -> str:
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)


class BadStdStreamFile(ParslError):
    """Error raised due to bad filepaths specified for STDOUT/ STDERR.

    Contains:
       reason(string)
       outputs(List of strings/files..)
       exception object
    """

    def __init__(self, outputs: List[Union[str, File]], exception: Exception):
        super().__init__(outputs, exception)
        self._outputs = outputs
        self._exception = exception

    def __repr__(self) -> str:
        return "FilePath: [{}] Exception: {}".format(self._outputs,
                                                     self._exception)

    def __str__(self) -> str:
        return self.__repr__()


class RemoteExceptionWrapper:
    def __init__(self, e_type: type, e_value: Exception, traceback: TracebackType):

        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self) -> None:

        t = dill.loads(self.e_type)

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug("Reraising exception of type {}".format(t))

        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()

        reraise(t, v, tb)


# TODO: I don't think this is correct. We need to constrain the type of the wrapper
# to that of the wrapped function, whereas this specification makes the wrapper
# untyped. That said, I found no evidence on the InterTubes that this is possible.
def wrap_error(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: object, **kwargs: object) -> Any:
        import sys
        from parsl.app.errors import RemoteExceptionWrapper
        try:
            return func(*args, **kwargs)
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())
    return wrapper
