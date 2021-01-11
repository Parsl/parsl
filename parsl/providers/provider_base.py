import os
from abc import ABCMeta, abstractmethod, abstractproperty
from enum import Enum
from typing import Any, List, Optional


class JobState(bytes, Enum):
    """Defines a set of states that a job can be in"""

    def __new__(cls, value, terminal, status_name):
        # noinspection PyArgumentList
        obj = bytes.__new__(cls, [value])
        obj._value_ = value
        obj.terminal = terminal
        obj.status_name = status_name
        return obj

    UNKNOWN = (0, False, "UNKNOWN")
    PENDING = (1, False, "PENDING")
    RUNNING = (2, False, "RUNNING")
    CANCELLED = (3, True, "CANCELLED")
    COMPLETED = (4, True, "COMPLETED")
    FAILED = (5, True, "FAILED")
    TIMEOUT = (6, True, "TIMEOUT")
    HELD = (7, False, "HELD")


class JobStatus(object):
    """Encapsulates a job state together with other details, presently a (error) message"""
    SUMMARY_TRUNCATION_THRESHOLD = 2048

    def __init__(self, state: JobState, message: str = None, exit_code: Optional[int] = None,
                 stdout_path: str = None, stderr_path: str = None):
        self.state = state
        self.message = message
        self.exit_code = exit_code
        self.stdout_path = stdout_path
        self.stderr_path = stderr_path

    @property
    def terminal(self):
        return self.state.terminal

    @property
    def status_name(self):
        return self.state.status_name

    def __repr__(self):
        if self.message is not None:
            return "{} ({})".format(self.state, self.message)
        else:
            return "{}".format(self.state)

    @property
    def stdout(self):
        return self._read_file(self.stdout_path)

    @property
    def stderr(self):
        return self._read_file(self.stderr_path)

    def _read_file(self, path):
        try:
            with open(path, 'r') as f:
                return f.read()
        except Exception:
            return None

    @property
    def stdout_summary(self):
        return self._read_summary(self.stdout_path)

    @property
    def stderr_summary(self):
        return self._read_summary(self.stderr_path)

    def _read_summary(self, path):
        if not path:
            # can happen for synthetic job failures
            return None
        try:
            with open(path, 'r') as f:
                f.seek(0, os.SEEK_END)
                size = f.tell()
                f.seek(0, os.SEEK_SET)
                if size > JobStatus.SUMMARY_TRUNCATION_THRESHOLD:
                    head = f.read(JobStatus.SUMMARY_TRUNCATION_THRESHOLD / 2)
                    f.seek(size - JobStatus.SUMMARY_TRUNCATION_THRESHOLD / 2, os.SEEK_SET)
                    tail = f.read(JobStatus.SUMMARY_TRUNCATION_THRESHOLD / 2)
                    return head + '\n...\n' + tail
                else:
                    f.seek(0, os.SEEK_SET)
                    return f.read()
        except FileNotFoundError:
            # When output is redirected to a file, but the process does not produce any output
            # bytes, no file is actually created. This handles that case.
            return None


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
    _cores_per_node = None  # type: Optional[int]
    _mem_per_node = None  # type: Optional[float]

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
