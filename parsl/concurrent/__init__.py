"""Interfaces modeled after Python's `concurrent library <https://docs.python.org/3/library/concurrent.html>`_"""
import time
from concurrent.futures import Executor
from contextlib import AbstractContextManager
from typing import Callable, Dict, Iterable, Iterator, Literal, Optional
from warnings import warn

from parsl import Config, DataFlowKernel, load
from parsl.app.python import PythonApp


class ParslPoolExecutor(Executor, AbstractContextManager):
    """An executor that uses a pool of workers managed by Parsl

    Works just like a :class:`~concurrent.futures.ProcessPoolExecutor` except that tasks
    are distributed across workers that can be on different machines.

    Create a new executor using one of two methods:

    1. Supplying a Parsl :class:`~parsl.Config` that defines how to create new workers.
       The executor will start a new Parsl Data Flow Kernel (DFK) when it is entered as a context manager.

    2. Supplying an already-started Parsl :class:`~parsl.DataFlowKernel` (DFK).
       The executor assumes you will start and stop the Parsl DFK outside the Executor.

    Note: Parsl does not support canceling tasks. The :meth:`map` method does not cancel work
    when one member of the run fails or a timeout is reached
    and :meth:`shutdown` does not cancel work on completion.
    """

    def __init__(self, config: Config | None = None, dfk: DataFlowKernel | None = None, executors: Literal['all'] | list[str] = 'all'):
        """Create the executor

        Args:
            config: Configuration for the Parsl Data Flow Kernel (DFK)
            dfk: DataFlowKernel of an already-started parsl
            executors: List of executors to use for supplied functions
        """
        if (config is not None) and (dfk is not None):
            raise ValueError('Specify only one of config or dfk')
        if (config is None) and (dfk is None):
            raise ValueError('Must specify one of config or dfk')
        self._config = config
        self._app_cache: Dict[Callable, PythonApp] = {}  # Cache specific to this instance: https://stackoverflow.com/questions/33672412
        self._dfk = dfk
        self.executors = executors

        # Start workers immediately
        if self._config is not None:
            self._dfk = load(self._config)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._dfk is None:  # Nothing has been started, do nothing
            return
        elif self._config is not None:  # The executors are being managed by this class, shut them down
            self.shutdown(wait=True)
            return
        else:  # The DFK is managed elsewhere, do nothing
            return

    @property
    def app_count(self):
        """Number of functions currently registered with the executor"""
        return len(self._app_cache)

    def get_app(self, fn: Callable) -> PythonApp:
        """Create a PythonApp for a function

        Args:
            fn: Function to be turned into a Parsl app
        Returns:
            PythonApp version of that function
        """
        if fn in self._app_cache:
            return self._app_cache[fn]
        app = PythonApp(fn, data_flow_kernel=self._dfk, executors=self.executors)
        self._app_cache[fn] = app
        return app

    def submit(self, fn, *args, **kwargs):
        if self._dfk is None:
            raise RuntimeError('Executor has been shut down.')
        app = self.get_app(fn)
        return app(*args, **kwargs)

    # TODO (wardlt): This override can go away when Parsl supports cancel
    def map(self, fn: Callable, *iterables: Iterable, timeout: Optional[float] = None, chunksize: int = 1) -> Iterator:
        # This is a version of the CPython 3.9 `.map` implementation modified to not use `cancel`
        if timeout is not None:
            end_time = timeout + time.monotonic()

        # Submit the applications
        app = self.get_app(fn)
        fs = [app(*args) for args in zip(*iterables)]

        # Yield the futures as completed
        def result_iterator():
            # reverse to keep finishing order
            fs.reverse()
            while fs:
                # Careful not to keep a reference to the popped future
                if timeout is None:
                    yield fs.pop().result()
                else:
                    yield fs.pop().result(end_time - time.monotonic())

        return result_iterator()

    def shutdown(self, wait: bool = True, *, cancel_futures: bool = False) -> None:
        if self._dfk is None:
            return  # Do nothing. Nothing is active
        if cancel_futures:
            warn(message="Canceling on-going tasks is not supported in Parsl")
        if wait:
            self._dfk.wait_for_current_tasks()
        if self._config is not None:  # The executors are being managed
            self._dfk.cleanup()  # Shutdown the DFK
        self._dfk = None
