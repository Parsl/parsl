from abc import ABCMeta, abstractmethod, abstractproperty
import logging
from typing import Any, Dict, List, Optional

from parsl.channels.base import Channel
from parsl.jobs.states import JobStatus

logger = logging.getLogger(__name__)


class ExecutionProvider(metaclass=ABCMeta):
    """Execution providers are responsible for managing execution resources
    that have a Local Resource Manager (LRM). For instance, campus clusters
    and supercomputers generally have LRMs (schedulers) such as Slurm,
    Torque/PBS, Condor and Cobalt. Clouds, on the other hand, have API
    interfaces that allow much more fine-grained composition of an execution
    environment. An execution provider abstracts these types of resources and
    provides a single uniform interface to them.

    The providers abstract away the interfaces provided by various systems to
    request, monitor, and cancel compute resources.

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

    @abstractmethod
    def __init__(self) -> None:
        self.min_blocks: int
        self.max_blocks: int
        self.init_blocks: int
        self.nodes_per_block: int
        self.script_dir: Optional[str]
        self.parallelism: float
        self.resources: Dict[object, Any]
        self._cores_per_node: Optional[int] = None
        self._mem_per_node: Optional[float] = None
        pass

    @abstractmethod
    def submit(self, command: str, tasks_per_node: int, job_name: str = "parsl.auto") -> object:
        ''' The submit method takes the command string to be executed upon
        instantiation of a resource most often to start a pilot (such as for
        HighThroughputExecutor or WorkQueueExecutor).

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
    def status(self, job_ids: List[object]) -> List[JobStatus]:
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
    def cancel(self, job_ids: List[object]) -> List[bool]:
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
        run concurrently per node.
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
        run concurrently per node.
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


class Channeled():
    """A marker type to indicate that parsl should manage a Channel for this provider"""
    def __init__(self) -> None:
        self.channel: Channel
        pass


class MultiChanneled():
    """A marker type to indicate that parsl should manage multiple Channels for this provider"""

    def __init__(self) -> None:
        self.channels: List[Channel]
        pass
