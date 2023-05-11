from parsl.errors import ParslError
from parsl.app.errors import AppException


class TaskVineTaskFailure(AppException):
    """A failure executing a task in taskvine

    Contains:
    reason(string)
    status(int)
    """

    def __init__(self, reason, status):
        self.reason = reason
        self.status = status


class TaskVineFailure(ParslError):
    """A failure in the taskvine executor that prevented the task to be
    executed.""
    """
    pass
