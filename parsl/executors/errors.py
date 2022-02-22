"""Exceptions raise by Executors."""
from parsl.app.errors import ParslError
from parsl.executors.base import ParslExecutor

from typing import Optional


class ExecutorError(ParslError):
    """Base class for executor related exceptions.

    Only to be invoked when only a more specific error is not available.
    """

    # TODO: this constructor doesn't make sense for most errors here
    # so it and the __str__ impl shoudl go away, and the few places
    # that an ExecutorError is directly instantiated shoudl be replaced
    # by a more specific errror
    def __init__(self, executor: ParslExecutor, reason):
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
        return "The {} feature is unsupported in {}. \
Please checkout {} for this feature".format(self.feature,
                                            self.current_executor,
                                            self.target_executor)


class ScalingFailed(ExecutorError):
    """Scaling failed due to error in Execution provider."""

    def __init__(self, executor_label: Optional[str], reason: str):
        self.executor_label = executor_label
        self.reason = reason

    def __str__(self):
        return "Executor {0} scaling failed due to: {1}".format(self.executor_label, self.reason)


class DeserializationError(ExecutorError):
    """ Failure at the Deserialization of results/exceptions from remote workers
    """

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return "Failed to deserialize return objects. Reason:{}".format(self.reason)


class SerializationError(ExecutorError):
    """ Failure to serialize data arguments for the tasks
    """

    def __init__(self, fname):
        self.fname = fname
        self.troubleshooting = "https://parsl.readthedocs.io/en/latest/faq.html#addressing-serializationerror"

    def __str__(self):
        return "Failed to serialize data objects for {}. Refer {} ".format(self.fname,
                                                                           self.troubleshooting)


class BadMessage(ExecutorError):
    """ Mangled/Poorly formatted/Unsupported message received
    """

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return "Received an unsupported message. Reason:{}".format(self.reason)
