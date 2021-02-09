"""Exceptions raise by Executors."""
from parsl.app.errors import ParslError

from typing import Optional


class ExecutorError(ParslError):
    """Base class for all exceptions.

    Only to be invoked when only a more specific error is not available.
    """

    def __init__(self, executor, reason):
        self.executor = executor
        self.reason = reason

    def __str__(self):
        return "Executor {0} failed due to: {1}".format(self.executor, self.reason)


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

    def __init__(self, executor: Optional[str], reason: str):
        self.executor = executor
        self.reason = reason


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
