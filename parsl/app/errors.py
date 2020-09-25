"""Exceptions raised by Apps."""
from functools import wraps
from typing import Callable, List, Union, Any, TypeVar
from types import TracebackType

import dill
import logging
from tblib import Traceback

from six import reraise

from parsl.data_provider.files import File

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

    def __init__(self, reason: str, exitcode: int) -> None:
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

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class MissingOutputs(ParslError):
    """Error raised at the end of app execution due to missing output files.

    Contains:
    reason(string)
    outputs(List of strings/files..)
    """

    def __init__(self, reason: str, outputs: List[Union[str, File]]) -> None:
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

    def __init__(self, reason: str, exception: Exception) -> None:
        super().__init__(reason, exception)
        self._reason = reason
        self._exception = exception

    def __repr__(self) -> str:
        return "Bad Stream File: {} Exception: {}".format(self._reason, self._exception)

    def __str__(self) -> str:
        return self.__repr__()


class RemoteExceptionWrapper:
    def __init__(self, e_type: type, e_value: Exception, traceback: TracebackType) -> None:

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


R = TypeVar('R')

# There appears to be no solutio to typing this without a mypy plugin.
# The reason is because wrap_error maps a Callable[[X...], R] to a Callable[[X...], Union[R, R2]].
# However, there is no provision in Python typing for pattern matching all possible types of
# callable arguments. This is because Callable[] is, in the infinite wisdom of the typing module,
# only used for callbacks: "There is no syntax to indicate optional or keyword arguments; such
# function types are rarely used as callback types.".
# The alternative supported by the typing module, of saying Callable[..., R] ->
#   Callable[..., Union[R, R2]] results in no pattern matching between the first and second
# ellipsis.
# Yet another bogus solution that was here previously would simply define wrap_error as
#   wrap_error(T) -> T, where T was a custom TypeVar. This obviously missed the fact that
# the returned function had its return signature modified.
# Ultimately, the best choice appears to be Callable[..., R] -> Callable[..., Union[R, ?Exception]],
#  since it results in the correct type specification for the return value(s) while treating the
#  arguments as Any.


def wrap_error(func: Callable[..., R]) -> Callable[..., Union[R, RemoteExceptionWrapper]]:
    @wraps(func)  # type: ignore
    def wrapper(*args: object, **kwargs: object) -> Any:
        import sys
        from parsl.app.errors import RemoteExceptionWrapper
        try:
            return func(*args, **kwargs)  # type: ignore
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())
    return wrapper  # type: ignore
