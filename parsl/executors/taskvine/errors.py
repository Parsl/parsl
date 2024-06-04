from parsl.app.errors import AppException
from parsl.errors import ParslError


class TaskVineTaskFailure(AppException):
    """A failure executing a task in TaskVine

    Contains:
    reason(string)
    status(int)
    """

    def __init__(self, reason: str, status: int):
        self.reason = reason
        self.status = status


class TaskVineManagerFailure(ParslError):
    """A failure in the taskvine executor that prevented the task to be
    executed.
    """
    pass


class TaskVineFactoryFailure(ParslError):
    """A failure in the TaskVine factory that prevents the factory from
    supplying workers to the manager.
    """
    pass
