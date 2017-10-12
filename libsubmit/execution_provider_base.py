from abc import ABCMeta, abstractmethod, abstractproperty

class ExecutionProvider(metaclass=ABCMeta):
    """ Define the strict interface for all Execution Provider

    .. code:: python

                                +------------------
                                |
          script_string ------->|  submit
               id      <--------|---+
                                |
          [ ids ]       ------->|  status
          [statuses]   <--------|----+
                                |
          [ ids ]       ------->|  cancel
          [cancel]     <--------|----+
                                |
          [True/False] <--------|  scaling_enabled
                                |
                                +-------------------
     """

    @abstractmethod
    def submit(self, *args, **kwargs):
        ''' We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn

        Returns:
             - A job identifier, this could be an integer, string etc

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''

        pass

    @abstractmethod
    def status(self, ids, **kwargs):
        ''' We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn

        Args:
             - A list of job identifiers

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT']

        Raises:
             - ExecutionProviderExceptions or its subclasses

        '''

        pass

    @abstractmethod
    def cancel(self, ids, **kwargs):
        ''' We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn

        Args:
             - A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderExceptions or its subclasses
        '''

        pass

    @abstractproperty
    def scaling_enabled(self):
        ''' The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        '''
        pass

    @abstractproperty
    def channels_required(self):
        ''' Returns Bool, depending on need for channels
        '''
        pass
