''' Exceptions raise by Apps.
'''
from parsl.errors import ParslError
from typing import Optional

# vs PR 1846
# there are differences in style between calling super.__init__ with
# parameters, or at all
# I think these are only stylistic

# there are some semantic differences - eg removal of exceptions from
# channel error base and adding into only the subclasses which have
# exceptions (can't remember what motivated this specifically)


class ChannelError(ParslError):
    """ Base class for all exceptions

    Only to be invoked when only a more specific error is not available.

    vs PR 1846: differs in calling of super.__init__ and I've removed
    the Exception parameter
    """
    def __init__(self, reason: str, hostname: str) -> None:
        super().__init__()
        self.reason = reason
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

    # vs PR 1846: removal of 'e' parameter in superclass
    # and stored in this class init instead
    def __init__(self, e, hostname):
        super().__init__("SSH channel could not be created since server's host keys could not be verified", hostname)
        self.e = e


class BadScriptPath(ChannelError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    # vs PR 1846, as with BadHostKeyException: remove e from superclass, store in this class
    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("Inaccessible remote script dir. Specify script_dir", hostname)
        self.e = e


class BadPermsScriptPath(ChannelError):
    ''' User does not have permissions to access the script_dir on the remote site

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    "vs PR 1846, store exception locally"
    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("User does not have permissions to access the script_dir", hostname)
        self.e = e


class FileExists(ChannelError):
    ''' Push or pull of file over channel fails since a file of the name already
    exists on the destination.

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    # vs PR 1846, PR 1846 uses .format instead of +filename. I previously kept the original behaviour
    # but am adopting the .format style here
    def __init__(self, e: Exception, hostname: str, filename: Optional[str] = None) -> None:
        super().__init__("File name collision in channel transport phase: {}".format(filename),
                         hostname)
        self.e = e


class AuthException(ChannelError):
    ''' An error raised during execution of an app.
    What this exception contains depends entirely on context
    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("Authentication to remote server failed", hostname)
        self.e = e


class SSHException(ChannelError):
    ''' if there was any other error connecting or establishing an SSH session

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("Error connecting or establishing an SSH session", hostname)
        self.e = e


class FileCopyException(ChannelError):
    ''' File copy operation failed

    Contains:
    reason(string)
    e (paramiko exception object)
    hostname (string)
    '''

    def __init__(self, e: Exception, hostname: str) -> None:
        super().__init__("File copy failed due to {0}".format(e), hostname)
        self.e = e
