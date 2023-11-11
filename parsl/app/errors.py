"""Exceptions raised by Apps."""
from functools import wraps
from typing import Callable, List, Optional, TypeVar, Union
from typing_extensions import ParamSpec
from types import TracebackType
import logging
from tblib import Traceback

from six import reraise

from parsl.data_provider.files import File
from parsl.errors import ParslError

logger = logging.getLogger(__name__)


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
    app name (str)
    exitcode (int)
    """

    def __init__(self, app_name: str, exitcode: int) -> None:
        self.app_name = app_name
        self.exitcode = exitcode

    def __str__(self) -> str:
        return f"bash_app {self.app_name} failed with unix exit code {self.exitcode}"


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
    outputs(List of files)
    """
    def __init__(self, reason: str, outputs: List[File]) -> None:
        super().__init__(reason, outputs)
        self.reason = reason
        self.outputs = outputs

    def __repr__(self) -> str:
        return "Missing Outputs: {0}, Reason:{1}".format(self.outputs, self.reason)


class BadStdStreamFile(ParslError):
    """Error raised due to bad filepaths specified for STDOUT/ STDERR.

    Contains:
       reason(string)
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
    def __init__(self, e_type: type, e_value: BaseException, traceback: Optional[TracebackType]) -> None:

        self.e_type = e_type
        self.e_value = e_value
        self.e_traceback = None if traceback is None else Traceback(traceback)
        if e_value.__cause__ is None:
            self.cause = None
        else:
            cause = e_value.__cause__
            self.cause = self.__class__(type(cause), cause, cause.__traceback__)

    def reraise(self) -> None:

        t = self.e_type

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug("Reraising exception of type {}".format(self.e_type))

        v = self.get_exception()

        reraise(t, v, v.__traceback__)

    def get_exception(self) -> BaseException:
        v = self.e_value
        if self.cause is not None:
            v.__cause__ = self.cause.get_exception()
        if self.e_traceback is not None:
            tb = self.e_traceback.as_traceback()
            return v.with_traceback(tb)
        else:
            return v


P = ParamSpec('P')
R = TypeVar('R')


def wrap_error(func: Callable[P, R]) -> Callable[P, Union[R, RemoteExceptionWrapper]]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Union[R, RemoteExceptionWrapper]:
        import sys
        from parsl.app.errors import RemoteExceptionWrapper
        try:
            return func(*args, **kwargs)
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())
    return wrapper
