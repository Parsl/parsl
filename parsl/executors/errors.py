"""Exceptions raise by Executors."""
from parsl.app.errors import ParslError


class ExecutorError(ParslError):
    """Base class for all exceptions.

    Only to be invoked when only a more specific error is not available.
    """

    def __init__(self, executor, reason):
        self.executor = executor
        self.reason = reason

    def __repr__(self):
        return "Executor {0} failed due to {1}".format(self.executor, self.reason)


class InsufficientMPIRanks(ExecutorError):
    ''' Error raised when attempting to launch a MPI worker pool with less than 2 ranks
    '''

    def __init__(self, tasks_per_node=None, nodes_per_block=None):
        self.tasks_per_node = tasks_per_node
        self.nodes_per_block = nodes_per_block

    def __repr__(self):
        return "MPIExecutor requires at least 2 ranks launched. \
Ranks launched = tasks_per_node={} X nodes_per_block={}".format(self.tasks_per_node,
                                                                self.nodes_per_block)


class ScalingFailed(ExecutorError):
    """Scaling failed due to error in Execution provider."""

    def __init__(self, executor, reason):
        self.executor = executor
        self.reason = reason


class ControllerError(ExecutorError):
    """Error raise by IPP controller."""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Controller init failed:Reason:{0}".format(self.reason)

    def __str__(self):
        return self.__repr__()


class DeserializationError(ExecutorError):
    """ Failure at the Deserialization of results/exceptions from remote workers
    """

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Failed to deserialize return objects. Reason:{}".format(self.reason)


class BadMessage(ExecutorError):
    """ Mangled/Poorly formatted/Unsupported message received
    """

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return "Received an unsupported message. Reason:{}".format(self.reason)
