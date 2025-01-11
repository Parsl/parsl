from typing import List, Optional, Sequence, Tuple

from parsl.errors import ParslError


class DataFlowException(ParslError):
    """Base class for all exceptions.

    Only to be invoked when only a more specific error is not available.

    """


class BadCheckpoint(DataFlowException):
    """Error raised at the end of app execution due to missing output files.

    Args:
         - reason

    Contains:
    reason (string)
    dependent_exceptions
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason

    def __repr__(self) -> str:
        return self.reason

    def __str__(self) -> str:
        return self.__repr__()


class DependencyError(DataFlowException):
    """Error raised if an app cannot run because there was an error
       in a dependency.

    Args:
         - dependent_exceptions_tids: List of exceptions and identifiers for
           dependencies which failed. The identifier might be a task ID or
           the repr of a non-DFK Future.
         - task_id: Task ID of the task that failed because of the dependency error
    """

    def __init__(self, dependent_exceptions_tids: Sequence[Tuple[Exception, str]], task_id: int) -> None:
        self.dependent_exceptions_tids = dependent_exceptions_tids
        self.task_id = task_id

        (cause, cause_sequence) = self._find_any_root_cause()
        self.__cause__ = cause
        self._cause_sequence = cause_sequence

    def __str__(self) -> str:
        sequence_text = " <- ".join(self._cause_sequence)
        return f"Dependency failure for task {self.task_id}. Example task failure sequence {sequence_text}"

    def _find_any_root_cause(self) -> Tuple[BaseException, List[str]]:
        """Looks recursively through self.dependent_exceptions_tids to find
        an exception that caused this dependency failure, that is not itself
        a dependency failure.
        """
        e: BaseException = self
        dep_ids = []
        while isinstance(e, DependencyError) and len(e.dependent_exceptions_tids) >= 1:
            dep_ids.append(e.dependent_exceptions_tids[0][1])
            e = e.dependent_exceptions_tids[0][0]
        return e, dep_ids


# TODO: this exception should get the same treatment as dependency error
class JoinError(DataFlowException):
    """Error raised if apps joining into a join_app raise exceptions.
       There can be several exceptions (one from each joining app),
       and JoinError collects them all together.
    """
    def __init__(self, dependent_exceptions_tids: Sequence[Tuple[BaseException, Optional[str]]], task_id: int) -> None:
        self.dependent_exceptions_tids = dependent_exceptions_tids
        self.task_id = task_id

    def __str__(self) -> str:
        dep_tids = [tid for (exception, tid) in self.dependent_exceptions_tids]
        return "Join failure for task {} with failed join dependencies from tasks {}".format(self.task_id, dep_tids)
