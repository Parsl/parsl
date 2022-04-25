"""Exceptions raise by Executors."""
from parsl.app.errors import ParslError
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
        return "The {} feature is unsupported in {}. \
Please checkout {} for this feature".format(self.feature,
                                            self.current_executor,
                                            self.target_executor)


class ScalingFailed(ExecutorError):
    """Scaling failed due to error in Execution provider."""

    def __str__(self):
        return f"Executor {self.executor.label} failed to scale due to: {self.reason}"


class DeserializationError(ParslError):
    """ Failure at the Deserialization of results/exceptions from remote workers
    """

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return "Failed to deserialize return objects. Reason:{}".format(self.reason)


class SerializationError(ParslError):
    """ Failure to serialize data arguments for the tasks
    """

    def __init__(self, fname):
        self.fname = fname
        self.troubleshooting = "https://parsl.readthedocs.io/en/latest/faq.html#addressing-serializationerror"

    def __str__(self):
        return "Failed to serialize data objects for {}. Refer {} ".format(self.fname,
                                                                           self.troubleshooting)


class BadMessage(ParslError):
    """ Mangled/Poorly formatted/Unsupported message received
    """

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return "Received an unsupported message. Reason:{}".format(self.reason)
