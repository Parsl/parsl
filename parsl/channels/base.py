from abc import ABCMeta, abstractmethod, abstractproperty
from typing import Tuple


class Channel(metaclass=ABCMeta):
    """Channels are abstractions that enable ExecutionProviders to talk to
    resource managers of remote compute facilities.

    For certain resources such as campus clusters or supercomputers at
    research laboratories, resource requirements may require authentication.

    The only remaining Channel, *LocalChannel*, executes commands locally in a
    shell.

    Channels provide the ability to execute commands remotely, using the
    execute_wait method, and manipulate the remote file system using methods
    such as push_file, pull_file and makedirs.

    Channels should ensure that each launched command runs in a new process
    group, so that providers (such as LocalProvider) which terminate long
    running commands using process groups can do so.
    """

    @abstractmethod
    def execute_wait(self, cmd: str, walltime: int = 0) -> Tuple[int, str, str]:
        ''' Executes the cmd, with a defined walltime.

        Args:
            - cmd (string): Command string to execute over the channel
            - walltime (int) : Timeout in seconds

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
