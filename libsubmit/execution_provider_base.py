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
        '''

        pass

    @abstractmethod
    def status(self, ids, **kwargs):
        ''' We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn
        '''

        pass

    @abstractmethod
    def cancel(self, ids, **kwargs):
        ''' We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn
        '''

        pass

    @abstractproperty
    def scaling_enabled(self):
        ''' The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        '''
        pass
