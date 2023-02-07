import parsl.app.errors as perror


class TaskVineTaskFailure(perror.AppException):
    """A failure executing a task in taskvine

    Contains:
    reason(string)
    status(int)
    """

    def __init__(self, reason, status):
        self.reason = reason
        self.status = status


class TaskVineFailure(perror.ParslError):
    """A failure in the taskvine executor that prevented the task to be
    executed.""
    """
    pass
