from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import Future

from typing import Any, Callable, Optional


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

    @abstractmethod
    def start(self) -> None:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func: Callable, *args: Any, **kwargs: Any) -> Future:
        """Submit.

        The value returned must be a Future, with the further requirements that
        it must be possible to assign a retries_left member slot to that object.
        """
        pass

    @abstractmethod
    def scale_out(self, blocks: int) -> None:
        """Scale out method.

        We should have the scale out method simply take resource object
        which will have the scaling methods, scale_out itself should be a coroutine, since
        scaling tasks can be slow.
        """
        pass

    @abstractmethod
    def scale_in(self, blocks: int) -> None:
        """Scale in method.

        Cause the executor to reduce the number of blocks by count.

        We should have the scale in method simply take resource object
        which will have the scaling methods, scale_in itself should be a coroutine, since
        scaling tasks can be slow.
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
