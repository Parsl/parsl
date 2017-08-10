from abc import ABCMeta, abstractmethod, abstractproperty

class ParslExecutor(metaclass=ABCMeta):
    """ Define the strict interface for all Executor classes
    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.

    .. note:: Shutdown is currently missing, as it is not yet supported by some of the executors (threads for eg).
    
    """

    @abstractmethod
    def submit(self, *args, **kwargs):
        ''' We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn
        '''

        pass

    @abstractmethod
    def scale_out(self, *args, **kwargs):
        ''' Scale out method. We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.
        '''
        pass

    @abstractmethod
    def scale_in(self, *args, **kwargs):
        ''' Scale in method. We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a corinine, since
        scaling tasks can be slow.
        '''

        pass

    @abstractproperty
    def scaling_enabled(self):
        ''' The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        '''
        pass
