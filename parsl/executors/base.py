from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import Future
from typing import Any, Callable, Dict, Optional, List

from parsl.providers.provider_base import JobStatus


class ParslExecutor(metaclass=ABCMeta):
    """Define the strict interface for all Executor classes.

    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.

    In addition to the listed methods, a ParslExecutor instance must always
    have a member field:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

    An executor may optionally expose:

       storage_access: List[parsl.data_provider.staging.Staging] - a list of staging
              providers that will be used for file staging. In the absence of this
              attribute, or if this attribute is `None`, then a default value of
              `parsl.data_provider.staging.default_staging` will be used by the
              staging code.

              Typechecker note: Ideally storage_access would be declared on executor
              __init__ methods as List[Staging] - however, lists are by default
              invariant, not co-variant, and it looks like @typeguard cannot be
              persuaded otherwise. So if you're implementing an executor and want to
              @typeguard the constructor, you'll have to use List[Any] here.
    """

    # This __init__ method exists to provide a python 3.5 compatible type declaration
    # for the `label` attribute. If/when python 3.5 is deprecated (see parsl issue #1553)
    # this could be replaced with a python 3.6 style attribute type declaration.
    #   label: str
    # and __init__ removed.

    @abstractmethod
    def __init__(self) -> None:
        self.label = ""  # type: str

    @abstractmethod
    def start(self) -> None:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func: Callable, resource_specification: Dict[str, Any], *args: Any, **kwargs: Any) -> Future:
        """Submit.
        """
        pass

    @abstractmethod
    def scale_out(self, blocks: int) -> List[object]:
        """Scale out method.

        We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.

        :return: A list of job ids corresponding to the blocks that were added.
        """
        pass

    @abstractmethod
    def scale_in(self, blocks: int) -> List[object]:
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.

        :return: A list of job ids corresponding to the blocks that were removed.
        """
        pass

    @abstractmethod
    def shutdown(self) -> bool:
        """Shutdown the executor.

        This includes all attached resources such as workers and controllers.
        """
        pass

    @abstractproperty
    def scaling_enabled(self) -> bool:
        """Specify if scaling is enabled.

        The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        """
        pass

    @abstractmethod
    def status(self) -> Dict[object, JobStatus]:
        """Return the status of all jobs/blocks currently known to this executor.

        :return: a dictionary mapping job ids to status strings
        """
        pass

    @property
    @abstractmethod
    def status_polling_interval(self) -> int:
        """Returns the interval, in seconds, at which the status method should be called. The
        assumption here is that, once initialized, an executor's polling interval is fixed.
        In practice, at least given the current situation, the executor uses a single task provider
        and this method is a delegate to the corresponding method in the provider.

        :return: the number of seconds to wait between calls to status() or zero if no polling
        should be done
        """
        pass

    @abstractmethod
    def set_bad_state_and_fail_all(self, exception: Exception):
        """Allows external error handlers to mark this executor as irrecoverably bad and cause
        all tasks submitted to it now and in the future to fail. The executor is responsible
        for checking  :method:bad_state_is_set() in the :method:submit() method and raising the
        appropriate exception, which is available through :method:executor_exception().
        """
        pass

    @property
    @abstractmethod
    def bad_state_is_set(self) -> bool:
        """Returns true if this executor is in an irrecoverable error state. If this method
        returns true, :property:executor_exception should contain an exception indicating the
        cause.
        """
        pass

    @property
    @abstractmethod
    def executor_exception(self) -> Exception:
        """Returns an exception that indicates why this executor is in an irrecoverable state."""
        pass

    @property
    @abstractmethod
    def tasks(self) -> Dict[object, Future]:
        """Contains a dictionary mapping task IDs to the corresponding Future objects for all
        tasks that have been submitted to this executor."""
        pass

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
