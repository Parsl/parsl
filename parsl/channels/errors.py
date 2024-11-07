''' Exceptions raise by Apps.
'''
from parsl.errors import ParslError


class ChannelError(ParslError):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    def __init__(self, reason: str, e: Exception, hostname: str) -> None:
        self.reason = reason
        self.e = e
        self.hostname = hostname

    def __str__(self) -> str:
        return "Hostname:{0}, Reason:{1}".format(self.hostname, self.reason)


class FileCopyException(ChannelError):
    ''' File copy operation failed

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("File copy failed due to {0}".format(e), e, hostname)
