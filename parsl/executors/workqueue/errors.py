from typing import Optional

from parsl.errors import ParslError
from parsl.app.errors import AppException


class WorkQueueTaskFailure(AppException):
    """A failure executing a task in workqueue

    Contains:
    reason(string)
    status(optional exception)
    """

    def __init__(self, reason: str, status: Optional[Exception]):
        self.reason = reason
        self.status = status


class WorkQueueFailure(ParslError):
    """A failure in the work queue executor that prevented the task to be
    executed.""
    """
    pass
