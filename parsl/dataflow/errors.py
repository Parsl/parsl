from typing import List, Sequence, Tuple

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

    def __str__(self) -> str:
        return self.reason


class PropagatedException(DataFlowException):
    """Error raised if an app fails because there was an error
       in a related task. This is intended to be subclassed for
       dependency and join_app errors.

    Args:
         - dependent_exceptions_tids: List of exceptions and brief descriptions
           for dependencies which failed. The description might be a task ID or
           the repr of a non-AppFuture.
         - task_id: Task ID of the task that failed because of the dependency error
    """

    def __init__(self,
                 dependent_exceptions_tids: Sequence[Tuple[BaseException, str]],
                 task_id: int,
                 *,
                 failure_description: str) -> None:
        self.dependent_exceptions_tids = dependent_exceptions_tids
        self.task_id = task_id
        self._failure_description = failure_description

        (cause, cause_sequence) = self._find_any_root_cause()
        self.__cause__ = cause
        self._cause_sequence = cause_sequence

    def __str__(self) -> str:
        sequence_text = " <- ".join(self._cause_sequence)
        return f"{self._failure_description} for task {self.task_id}. " \
               f"The representative cause is via {sequence_text}"

    def _find_any_root_cause(self) -> Tuple[BaseException, List[str]]:
        """Looks recursively through self.dependent_exceptions_tids to find
        an exception that caused this propagated error, that is not itself
        a propagated error.
        """
        e: BaseException = self
        dep_ids = []
        while isinstance(e, PropagatedException) and len(e.dependent_exceptions_tids) >= 1:
            id_txt = e.dependent_exceptions_tids[0][1]
            assert isinstance(id_txt, str)
            # if there are several causes for this exception, label that
            # there are more so that we know that the representative fail
            # sequence is not the full story.
            if len(e.dependent_exceptions_tids) > 1:
                id_txt += " (+ others)"
            dep_ids.append(id_txt)
            e = e.dependent_exceptions_tids[0][0]
        return e, dep_ids


class DependencyError(PropagatedException):
    """Error raised if an app cannot run because there was an error
       in a dependency. There can be several exceptions (one from each
       dependency) and DependencyError collects them all together.

    Args:
         - dependent_exceptions_tids: List of exceptions and brief descriptions
           for dependencies which failed. The description might be a task ID or
           the repr of a non-AppFuture.
         - task_id: Task ID of the task that failed because of the dependency error
    """
    def __init__(self, dependent_exceptions_tids: Sequence[Tuple[BaseException, str]], task_id: int) -> None:
        super().__init__(dependent_exceptions_tids, task_id,
                         failure_description="Dependency failure")


class JoinError(PropagatedException):
    """Error raised if apps joining into a join_app raise exceptions.
       There can be several exceptions (one from each joining app),
       and JoinError collects them all together.
    """
    def __init__(self, dependent_exceptions_tids: Sequence[Tuple[BaseException, str]], task_id: int) -> None:
        super().__init__(dependent_exceptions_tids, task_id,
                         failure_description="Join failure")
