from abc import ABCMeta, abstractmethod, abstractproperty


class ExecutionProvider(metaclass=ABCMeta):
    """ Define the strict interface for all Execution Providers

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
    def submit(self, command, tasks_per_node, job_name="parsl.auto"):
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot (such as IPP engine
        or even Swift-T engines).

        Args :
             - command (str) : The bash command string to be executed
             - tasks_per_node (int) : command invocations to be launched per node

        KWargs:
             - job_name (str) : Human friendly name to be assigned to the job request

        Returns:
             - A job identifier, this could be an integer, string etc
               or None or any other object that evaluates to boolean false
                  if submission failed but an exception isn't thrown.

        Raises:
             - ExecutionProviderException or its subclasses
        '''

        pass

    @abstractmethod
    def status(self, job_ids):
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of status from ['PENDING', 'RUNNING', 'CANCELLED', 'COMPLETED',
               'FAILED', 'TIMEOUT'] corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        '''

        pass

    @abstractmethod
    def cancel(self, job_ids):
        ''' Cancels the resources identified by the job_ids provided by the user.

        Args:
             - job_ids (list): A list of job identifiers

        Returns:
             - A list of status from cancelling the job which can be True, False

        Raises:
             - ExecutionProviderException or its subclasses
        '''

        pass

    @abstractproperty
    def scaling_enabled(self):
        ''' The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider

        Returns:
              - Status (Bool)
        '''

        pass

    @abstractproperty
    def label(self):
        ''' Provides the label for this provider '''
        pass

    def mem_per_node(self):
        """Real memory to provision per node in GB.

        If set to None, no explicit request will be made.
        """
        return None

    def cores_per_node(self):
        """Number of cores to provision per node.

        If set to None, executors will assume all cores on the node are
        available for computation.
        """
        return None
