import parsl.app.errors as perror


class WorkQueueTaskFailure(perror.AppException):
    """A failure executing a task in workqueue

    Contains:
    reason(string)
    status(int)
    """

    def __init__(self, reason, status):
        self.reason = reason
        self.status = status
