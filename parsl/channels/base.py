from abc import ABCMeta, abstractmethod, abstractproperty


class Channel(metaclass=ABCMeta):
    """For certain resources such as campus clusters or supercomputers at
    research laboratories, resource requirements may require authentication.
    For instance some resources may allow access to their job schedulers from
    only their login-nodes which require you to authenticate on through SSH,
    GSI-SSH and sometimes even require two factor authentication. Channels are
    simple abstractions that enable the ExecutionProvider component to talk to
    the resource managers of compute facilities. The simplest Channel,
    *LocalChannel*, simply executes commands locally on a shell, while the
    *SshChannel* authenticates you to remote systems.

    Channels are usually called via the execute_wait function.
    For channels that execute remotely, a push_file function allows you to copy over files.

    .. code:: python

                                +------------------
                                |
          cmd, wtime    ------->|  execute_wait
          (ec, stdout, stderr)<-|---+
                                |
          src, dst_dir  ------->|  push_file
             dst_path  <--------|----+
                                |
          dst_script_dir <------|  script_dir
                                |
                                +-------------------


    Channels should ensure that each launched command runs in a new process
    group, so that providers (such as AdHocProvider and LocalProvider) which
    terminate long running commands using process groups can do so.
    """

    @abstractmethod
    def execute_wait(self, cmd, walltime=None, envs={}, *args, **kwargs):
        ''' Executes the cmd, with a defined walltime.

        Args:
            - cmd (string): Command string to execute over the channel
            - walltime (int) : Timeout in seconds, optional

        KWargs:
            - envs (Dict[str, str]) : Environment variables to push to the remote side

        Returns:
            - (exit_code, stdout, stderr) (int, optional string, optional string)
              If the exit code is a failure code, the stdout and stderr return values
              may be None.
        '''
        pass

    @abstractproperty
    def script_dir(self):
        ''' This is a property. Returns the directory assigned for storing all internal scripts such as
        scheduler submit scripts. This is usually where error logs from the scheduler would reside on the
        channel destination side.

        Args:
            - None

        Returns:
            - Channel script dir
        '''
        pass

    @abstractmethod
    def push_file(self, source, dest_dir):
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
    def pull_file(self, remote_source, local_dir):
        ''' Transport file on the remote side to a local directory

        Args:
            remote_source (string): remote_source
            local_dir (string): Local directory to copy to


        Returns:
            destination_path (string)
        '''
        pass

    @abstractmethod
    def close(self):
        ''' Closes the channel. Clean out any auth credentials.

        Args:
            None

        Returns:
            Bool

        '''
        pass

    @abstractmethod
    def makedirs(self, path, mode=511, exist_ok=False):
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
    def isdir(self, path):
        """Return true if the path refers to an existing directory.

        Parameters
        ----------
        path : str
            Path of directory to check.
        """
        pass

    @abstractmethod
    def abspath(self, path):
        """Return the absolute path.

        Parameters
        ----------
        path : str
            Path for which the absolute path will be returned.
        """
        pass
