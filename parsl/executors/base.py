from abc import ABCMeta, abstractmethod, abstractproperty
from concurrent.futures import Future

from typing import Any, List, Optional


# for type checking:
from parsl.providers.provider_base import ExecutionProvider


class ParslExecutor(metaclass=ABCMeta):
    """Define the strict interface for all Executor classes.

    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.

    In addition to the listed methods, a ParslExecutor instance must always
    have a member field:

       label: str - a human readable label for the executor, unique
              with respect to other executors.

    """

    # mypy doesn't actually check that the below are defined by
    # concrete subclasses - see  github.com/python/mypy/issues/4426
    # and maybe PEP-544 Protocols

    label: str
    provider: ExecutionProvider
    managed: bool
    status: Any  # what is this? used by strategy
    outstanding: Any  # what is this? used by strategy
    working_dir: Optional[str]
    storage_access: List[Any]

    @abstractmethod
    def start(self) -> None:
        """Start the executor.

        Any spin-up operations (for example: starting thread pools) should be performed here.
        """
        pass

    @abstractmethod
    def submit(self, func, *args, **kwargs) -> Future:
        """Submit.

        We haven't yet decided on what the args to this can be,
        whether it should just be func, args, kwargs or be the partially evaluated
        fn

        BENC: based on how ipp uses this, this follows the semantics of async_apply from ipyparallel.
        Based on how the thread executor works, its:

            https://docs.python.org/3/library/concurrent.futures.html
            Schedules the callable, fn, to be executed as fn(*args **kwargs) and returns a Future object representing the execution of the callable.

        These are consistent

        The value returned must be some kind of future that I'm a bit vague on the
        strict requirements for:

             it must be possible to assign a retries_left member slot to that object.
             it's referred to as exec_fu - but it's whatever the underlying executor returns (ipp, thread pools, whatever) which has some Future-like behaviour
                  - so is it always the case that we can add retries_left? (I guess the python model permits that but it's a bit type-ugly)


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
    def scaling_enabled(self):
        """Specify if scaling is enabled.

        The callers of ParslExecutors need to differentiate between Executors
        and Executors wrapped in a resource provider
        """
        pass

    @property
    def run_dir(self):
        """Path to the run directory.
        """
        return self._run_dir

    @run_dir.setter
    def run_dir(self, value):
        self._run_dir = value
