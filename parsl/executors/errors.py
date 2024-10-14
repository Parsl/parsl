"""Exceptions raise by Executors."""
from typing import Set

from parsl.errors import ParslError
from parsl.executors.base import ParslExecutor


class ExecutorError(ParslError):
    """Base class for executor related exceptions.

    Only to be invoked when only a more specific error is not available.
    """

    def __init__(self, executor: ParslExecutor, reason: str):
        self.executor = executor
        self.reason = reason

    def __str__(self):
        return "Executor {0} failed due to: {1}".format(self.executor.label, self.reason)


class BadStateException(ExecutorError):
    """Error returned by task Futures when an executor is in a bad state.
    """

    def __init__(self, executor, exception):
        super().__init__(executor, str(exception))


class UnsupportedFeatureError(ExecutorError):
    """Error raised when attemping to use unsupported feature in an Executor"""

    def __init__(self, feature, current_executor, target_executor):
        self.feature = feature
        self.current_executor = current_executor
        self.target_executor = target_executor

    def __str__(self):
        if self.target_executor:
            return "The {} feature is unsupported in {}. \
                    Please checkout {} for this feature".format(self.feature,
                                                                self.current_executor,
                                                                self.target_executor)
        else:
            return "The {} feature is unsupported in {}.".format(self.feature,
                                                                 self.current_executor)


class InvalidResourceSpecification(ExecutorError):
    """Error raised when Invalid input is supplied via resource Specification"""

    def __init__(self, invalid_keys: Set[str], message: str = ''):
        self.invalid_keys = invalid_keys
        self.message = message

    def __str__(self):
        return f"Invalid Resource Specification Supplied: {self.invalid_keys}. {self.message}"


class ScalingFailed(ExecutorError):
    """Scaling failed due to error in Execution provider."""

    def __str__(self):
        return f"Executor {self.executor.label} failed to scale due to: {self.reason}"


class BadMessage(ParslError):
    """ Mangled/Poorly formatted/Unsupported message received
    """

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return "Received an unsupported message. Reason:{}".format(self.reason)
