from abc import ABCMeta, abstractmethod, abstractproperty
from typing import Dict, Tuple


class Channel(metaclass=ABCMeta):
    """Channels are abstractions that enable ExecutionProviders to talk to
    resource managers of remote compute facilities.

    For certain resources such as campus clusters or supercomputers at
    research laboratories, resource requirements may require authentication.
    For instance some resources may allow access to their job schedulers from
    only their login-nodes which require you to authenticate through SSH, or
    require two factor authentication.

    The simplest Channel, *LocalChannel*, executes commands locally in a
    shell, while the *SSHChannel* authenticates you to remote systems.

    Channels provide the ability to execute commands remotely, using the
    execute_wait method, and manipulate the remote file system using methods
    such as push_file, pull_file and makedirs.

    Channels should ensure that each launched command runs in a new process
    group, so that providers (such as AdHocProvider and LocalProvider) which
    terminate long running commands using process groups can do so.
    """

    @abstractmethod
    def execute_wait(self, cmd: str, walltime: int = 0, envs: Dict[str, str] = {}) -> Tuple[int, str, str]:
        ''' Executes the cmd, with a defined walltime.

        Args:
            - cmd (string): Command string to execute over the channel
            - walltime (int) : Timeout in seconds

        KWargs:
            - envs (Dict[str, str]) : Environment variables to push to the remote side

        Returns:
            - (exit_code, stdout, stderr) (int, string, string)
        '''
        pass

    @abstractproperty
    def script_dir(self) -> str:
        ''' This is a property. Returns the directory assigned for storing all internal scripts such as
        scheduler submit scripts. This is usually where error logs from the scheduler would reside on the
        channel destination side.

        Args:
            - None

        Returns:
            - Channel script dir
        '''
        pass

    # DFK expects to be able to modify this, so it needs to be in the abstract class
    @script_dir.setter
    def script_dir(self, value: str) -> None:
        pass

    @abstractmethod
    def push_file(self, source: str, dest_dir: str) -> str:
        ''' Channel will take care of moving the file from source to the destination
        directory

        Args:
            source (string) : Full filepath of the file to be moved
            dest_dir (string) : Absolute path of the directory to move to

        Returns:
            destination_path (string)
        '''
        pass

    @abstractmethod
    def pull_file(self, remote_source: str, local_dir: str) -> str:
        ''' Transport file on the remote side to a local directory

        Args:
            remote_source (string): remote_source
            local_dir (string): Local directory to copy to


        Returns:
            destination_path (string)
        '''
        pass

    @abstractmethod
    def close(self) -> None:
        ''' Closes the channel.
        '''
        pass

    @abstractmethod
    def makedirs(self, path: str, mode: int = 0o511, exist_ok: bool = False) -> None:
        """Create a directory.

        If intermediate directories do not exist, they will be created.

        Parameters
        ----------
        path : str
            Path of directory to create.
        mode : int
            Permissions (posix-style) for the newly-created directory.
        exist_ok : bool
            If False, raise an OSError if the target directory already exists.
        """
        pass

    @abstractmethod
    def isdir(self, path: str) -> bool:
        """Return true if the path refers to an existing directory.

        Parameters
        ----------
        path : str
            Path of directory to check.
        """
        pass

    @abstractmethod
    def abspath(self, path: str) -> str:
        """Return the absolute path.

        Parameters
        ----------
        path : str
            Path for which the absolute path will be returned.
        """
        pass
