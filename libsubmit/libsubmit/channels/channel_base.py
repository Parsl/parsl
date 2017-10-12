from abc import ABCMeta, abstractmethod, abstractproperty

class Channel (metaclass=ABCMeta):
    """ Define the interface to all channels

    .. code:: python

         execute_wait...

    """
    @abstractmethod
    def execute_wait(self, cmd, walltime, *args, **kwargs):
        '''
        Args:
            - cmd (string): Command string to execute over the channel
            - walltime (int) : Timeout in seconds

        Returns:
            - (exit_code, stdout, stderr) (int, string, string)
        '''
        pass

    @abstractproperty
    def script_dir(self):
        '''
        Args:
            - None

        Returns:
            - Channel script dir
        '''
        pass

    @abstractmethod
    def execute_no_wait(self, cmd, walltime, *args, **kwargs):
        ''' Optional. THis is infrequently used.

        Args:
            - cmd (string): Command string to execute over the channel
            - walltime (int) : Timeout in seconds

        Returns:
            - (exit_code(None), stdout, stderr) (int, io_thing, io_thing)
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
    def close(self):
        '''
        '''
        pass
