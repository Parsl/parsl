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
    _cores_per_node = None
    _mem_per_node = None

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

    @property
    def mem_per_node(self):
        """Real memory to provision per node in GB.

        Providers which set this property should ask for mem_per_node of memory
        when provisioning resources, and set the corresponding environment
        variable PARSL_MEMORY_GB before executing submitted commands.

        If this property is set, executors may use it to calculate how many tasks can
        run concurrently per node. This information is used by dataflow.Strategy to estimate
        the resources required to run all outstanding tasks.
        """
        return self._mem_per_node

    @mem_per_node.setter
    def mem_per_node(self, value):
        self._mem_per_node = value

    @property
    def cores_per_node(self):
        """Number of cores to provision per node.

        Providers which set this property should ask for cores_per_node cores
        when provisioning resources, and set the corresponding environment
        variable PARSL_CORES before executing submitted commands.

        If this property is set, executors may use it to calculate how many tasks can
        run concurrently per node. This information is used by dataflow.Strategy to estimate
        the resources required to run all outstanding tasks.
        """
        return self._cores_per_node

    @cores_per_node.setter
    def cores_per_node(self, value):
        self._cores_per_node = value
