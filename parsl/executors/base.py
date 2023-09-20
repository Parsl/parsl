from abc import ABCMeta, abstractmethod
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional, List
from typing_extensions import Literal, Self

from parsl.jobs.states import JobStatus

import parsl  # noqa F401


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

       storage_access: List[parsl.data_provider.staging.Staging] - a list of staging
              providers that will be used for file staging. In the absence of this
              attribute, or if this attribute is `None`, then a default value of
              ``parsl.data_provider.staging.default_staging`` will be used by the
              staging code.

              Typechecker note: Ideally storage_access would be declared on executor
              __init__ methods as List[Staging] - however, lists are by default
              invariant, not co-variant, and it looks like @typeguard cannot be
              persuaded otherwise. So if you're implementing an executor and want to
              @typeguard the constructor, you'll have to use List[Any] here.
    """

    label: str = "undefined"
    radio_mode: str = "udp"

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Literal[False]:
        self.shutdown()
        return False

    @abstractmethod
    def start(self) -> Optional[List[str]]:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """Submit.

        The executor can optionally set a parsl_executor_task_id attribute on
        the Future that it returns, and in that case, parsl will log a
        relationship between the executor's task ID and parsl level try/task
        IDs.
        """
        pass

    @abstractmethod
    def shutdown(self) -> bool:
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        pass

    def create_monitoring_info(self, status: Dict[str, JobStatus]) -> List[object]:
        """Create a monitoring message for each block based on the poll status.

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
