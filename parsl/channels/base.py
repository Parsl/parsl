from abc import ABCMeta, abstractmethod
from typing import Dict, Tuple


class Channel(metaclass=ABCMeta):
    """This is a base class for a feature that has now been removed from
    Parsl. This class should go away / be merged downwards into
    LocalChannel.
    """

    # Script dir must be an absolute path
    # This is initialized to None, which violates the type specification,
    # and so code using script_dir must make sure it doesn't expect this
    # to be not-None until after the DFK has initialized the channel
    script_dir: str

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
