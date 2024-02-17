from __future__ import annotations
from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional, List, Sequence, Union
from typing_extensions import Literal, Self

from parsl.jobs.states import JobStatus
from parsl.data_provider.staging import Staging

# for type checking:
from parsl.providers.base import ExecutionProvider
from typing_extensions import runtime_checkable, Protocol


class ParslExecutor(metaclass=ABCMeta):
    """Executors are abstractions that represent available compute resources
    to which you could submit arbitrary App tasks.

    This is an abstract base class that only enforces concrete implementations
    of functionality by the child classes.

    Can be used as a context manager. On exit, calls ``self.shutdown()`` with
    no arguments and re-raises any thrown exception.

    In addition to the listed methods, a ParslExecutor instance must always
    have a member field:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

    Per-executor monitoring behaviour can be influenced by exposing:

       radio_mode: str - a string describing which radio mode should be used to
              send task resource data back to the submit side.

    An executor may optionally expose:

       storage_access: Optional[Sequence[parsl.data_provider.staging.Staging]]
              A sequence of staging providers that will be used for file
              staging.  In the absence of this attribute, or if this
              attribute is `None`, then a default value of
              ``parsl.data_provider.staging.default_staging``
              will be used by the staging code.
    """

    label: str = "undefined"
    radio_mode: str = "udp"

    def __enter__(self) -> Self:
        return self

    # mypy doesn't actually check that the below are defined by
    # concrete subclasses - see  github.com/python/mypy/issues/4426
    # and maybe PEP-544 Protocols

    def __init__(self) -> None:
        self.label: str
        self.radio_mode: str = "udp"

        self.provider: Optional[ExecutionProvider] = None
        # this is wrong here. eg thread local executor has no provider.
        # perhaps its better attached to the block scaling provider?
        # cross-ref with notes of @property provider() in the
        # nostatushandlingexecutor.

        # there's an abstraction problem here - what kind of executor should
        # statically have this? for now I'll implement a protocol and assert
        # the protocol holds, wherever the code makes that assumption.
        # self.outstanding: int = None  # what is this? used by strategy
        self.working_dir: Optional[str] = None
        self.storage_access: Optional[Sequence[Staging]] = None
        self.run_id: Optional[str] = None

    # too lazy to figure out what the three Anys here should be
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self.shutdown()
        return False

    @abstractmethod
    def start(self) -> Optional[Sequence[str]]:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Dict[str, Any]) -> Future:
        """Submit.
        The executor can optionally set a parsl_executor_task_id attribute on
        the Future that it returns, and in that case, parsl will log a
        relationship between the executor's task ID and parsl level try/task
        IDs.
        """
        pass

    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        pass

    def create_monitoring_info(self, status: Dict[str, JobStatus]) -> List[object]:
        """Create a monitoring message for each block based on the poll status.

        TODO: block_id_type should be an enumerated list of valid strings, rather than all strings

        TODO: there shouldn't be any default values for this - when it is invoked, it should be explicit which is needed?
        Neither seems more natural to me than the other.

        TODO: internal vs external should be more clearly documented here

        :return: a list of dictionaries mapping to the info of each block
        """
        return []

    def monitor_resources(self) -> bool:
        """Should resource monitoring happen for tasks on running on this executor?

        Parsl resource monitoring conflicts with execution styles which use threads, and
        can deadlock while running.

        This function allows resource monitoring to be disabled per executor implementation.
        """
        return True

    @property
    def run_dir(self) -> str:
        """Path to the run directory.
        """
        return self._run_dir

    @run_dir.setter
    def run_dir(self, value: str) -> None:
        self._run_dir = value

    @property
    def hub_address(self) -> Optional[str]:
        """Address to the Hub for monitoring.
        """
        return self._hub_address

    @hub_address.setter
    def hub_address(self, value: Optional[str]) -> None:
        self._hub_address = value

    @property
    def hub_port(self) -> Optional[int]:
        """Port to the Hub for monitoring.
        """
        return self._hub_port

    @hub_port.setter
    def hub_port(self, value: Optional[int]) -> None:
        self._hub_port = value


@runtime_checkable
class HasWorkersPerNode(Protocol):
    """A marker type to indicate that the executor has a notion of workers per node. This maybe should merge into the block executor?"""
    @abstractproperty
    def workers_per_node(self) -> Union[int, float]:
        pass


class HasOutstanding:
    """A marker type to indicate that the executor has a count of outstanding tasks. This maybe should merge into the block executor?"""
    @abstractproperty
    def outstanding(self) -> int:
        pass


class FutureWithTaskID(Future):
    def __init__(self, task_id: str) -> None:
        super().__init__()
        self.parsl_executor_task_id = task_id
