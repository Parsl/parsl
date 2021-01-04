''' Exceptions raise by Apps.
'''
from typing import Optional


class ChannelError(Exception):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.
    """
    def __init__(self, reason: str, e: Exception, hostname: str) -> None:
        self.reason = reason
        self.e = e
        self.hostname = hostname

    def __repr__(self) -> str:
        return "Hostname:{0}, Reason:{1}".format(self.hostname, self.reason)

    def __str__(self) -> str:
        return self.__repr__()


class BadHostKeyException(ChannelError):
    ''' SSH channel could not be created since server's host keys could not
    be verified

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("SSH channel could not be created since server's host keys could not be "
                         "verified", e, hostname)


class BadScriptPath(ChannelError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("Inaccessible remote script dir. Specify script_dir", e, hostname)


class BadPermsScriptPath(ChannelError):
    ''' User does not have permissions to access the script_dir on the remote site

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("User does not have permissions to access the script_dir", e, hostname)


class FileExists(ChannelError):
    ''' Push or pull of file over channel fails since a file of the name already
    exists on the destination.

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str, filename: Optional[str] = None) -> None:
        super().__init__("File name collision in channel transport phase: {}".format(filename),
                         e, hostname)


class AuthException(ChannelError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("Authentication to remote server failed", e, hostname)


class SSHException(ChannelError):
    ''' if there was any other error connecting or establishing an SSH session

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("Error connecting or establishing an SSH session", e, hostname)


class FileCopyException(ChannelError):
    ''' File copy operation failed

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("File copy failed due to {0}".format(e), e, hostname)
