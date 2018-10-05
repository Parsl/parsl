from parsl.providers.error import ExecutionProviderException


class BadLauncher(ExecutionProviderException):
    """Error raised when a non callable object is provider as Launcher
    """

    def __init__(self, launcher, reason):
        self.launcher = launcher
        self.reason = reason

    def __repr__(self):
        return "Bad Launcher provided:{0} Reason:{1}".format(self.launcher, self.reason)
