from abc import ABCMeta, abstractmethod, abstractproperty

class Channel (metaclass=ABCMeta):
    """ Define the interface to all channels

    .. code:: python

         execute_wait...
    
    """
    @abstractmethod
    def execute_wait(self, cmd, walltime, *args, **kwargs):
        '''
        '''
        pass

    @abstractmethod
    def close(self):
        '''
        '''
        pass
