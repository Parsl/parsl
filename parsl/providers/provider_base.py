from abc import ABCMeta, abstractmethod, abstractproperty
from enum import Enum
from typing import Any, List, Optional

# for typechecking:
from typing import Any, Dict, List, Optional
from parsl.channels.base import Channel


# mypy can't typecheck the master version of JobState
# I hope that this behaves the same.
# The __repr__ is different at least - with master
# version: >>> JobState.RUNNING
# <JobState.RUNNING: 2>
# with this version:
# <JobState.RUNNING: (2, False)>

class JobState(Enum):
    """Defines a set of states that a job can be in"""

    UNKNOWN = (0, False)
    PENDING = (1, False)
    RUNNING = (2, False)
    CANCELLED = (3, True)
    COMPLETED = (4, True)
    FAILED = (5, True)
    TIMEOUT = (6, True)
    HELD = (7, False)

    @property
    def terminal(self) -> bool:
        (_, state) = self.value
        return state


class JobStatus(object):
    """Encapsulates a job state together with other details, presently a (error) message"""

    # mypy as I have configured it requires an explicit optional here.
    # there was a change in PEP484 that makes this behaviour itself different between
    # different typecheckers: https://github.com/python/peps/pull/689
    # previously = None implied Optional on the type, but there has been some pressure
    # against that - see https://github.com/python/typing/issues/275
    def __init__(self, state: JobState, message: Optional[str] = None):
        self.state = state
        self.message = message

    @property
    def terminal(self) -> bool:
        return self.state.terminal

    def __repr__(self) -> str:
        if self.message is not None:
            return "{} ({})".format(self.state, self.message)
        else:
            return "{}".format(self.state)


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
                                +-------------------
     """
    _cores_per_node = None  # type: Optional[int]
    _mem_per_node = None  # type: Optional[float]

    min_blocks: int
    max_blocks: int
    init_blocks: int
    nodes_per_block: int
    script_dir: Optional[str]
    parallelism: float #TODO not sure about this one?
    resources: Dict[Any, Any] # I think the contents of this are provider-specific?    


    @abstractmethod
    def submit(self, command: str, tasks_per_node: int, job_name: str = "parsl.auto") -> Any:
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
    def status(self, job_ids: List[Any]) -> List[JobStatus]:
        ''' Get the status of a list of jobs identified by the job identifiers
        returned from the submit request.

        Args:
             - job_ids (list) : A list of job identifiers

        Returns:
             - A list of JobStatus objects corresponding to each job_id in the job_ids list.

        Raises:
             - ExecutionProviderException or its subclasses

        '''

        pass

    @abstractmethod
    def cancel(self, job_ids: List[Any]) -> List[bool]:
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
    def label(self) -> str:
        ''' Provides the label for this provider '''
        pass


    @property
    def mem_per_node(self) -> Optional[float]:
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
    def mem_per_node(self, value: float) -> None:
        self._mem_per_node = value

    @property
    def cores_per_node(self) -> Optional[int]:
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
    def cores_per_node(self, value: int) -> None:
        self._cores_per_node = value

    @property
    @abstractmethod
    def status_polling_interval(self) -> int:
        """Returns the interval, in seconds, at which the status method should be called.

        :return: the number of seconds to wait between calls to status()
        """
        pass
