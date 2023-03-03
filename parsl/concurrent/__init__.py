"""Interfaces modeled after Python's `concurrent library <https://docs.python.org/3/library/concurrent.html>`_"""
from typing import Callable, Dict, Optional, Iterator, Iterable
from concurrent.futures import Executor
from warnings import warn
import time

from parsl import Config, DataFlowKernel
from parsl.app.python import PythonApp


class ParslPoolExecutor(Executor):
    """An executor that uses a pool of workers managed by Parsl

    Works just like a :class:`~concurrent.futures.ProcessPoolExecutor` except that tasks
    are distributed across workers that can be on different machines.
    Create a new executor by supplying a Parsl :class:`~parsl.Config` object to define
    how to create new workers, Parsl will set up and tear down workers on your behalf.

    Note: Parsl does not support canceling tasks. The :meth:`map` method does not cancel work
    when one member of the run fails or a timeout is reached
    and :meth:`shutdown` does not cancel work on completion.
    """

    def __init__(self, config: Config):
        """Create the executor

        Args:
            config: Configuration for the Parsl Data Flow Kernel (DFK)
        """
        self._config = config
        self.dfk = DataFlowKernel(self._config)
        self._app_cache: Dict[Callable, PythonApp] = {}  # Cache specific to this instance: https://stackoverflow.com/questions/33672412

    @property
    def app_count(self):
        """Number of functions currently registered with the executor"""
        return len(self._app_cache)

    def _get_app(self, fn: Callable) -> PythonApp:
        """Create a PythonApp for a function

        Args:
            fn: Function to be turned into a Parsl app
        Returns:
            PythonApp version of that function
        """
        if fn in self._app_cache:
            return self._app_cache[fn]
        app = PythonApp(fn, data_flow_kernel=self.dfk)
        self._app_cache[fn] = app
        return app

    def submit(self, fn, *args, **kwargs):
        app = self._get_app(fn)
        return app(*args, **kwargs)

    # TODO (wardlt): This override can go away when Parsl supports cancel
    def map(self, fn: Callable, *iterables: Iterable, timeout: Optional[float] = None, chunksize: int = 1) -> Iterator:
        # This is a version of the CPython 3.9 `.map` implementation modified to not use `cancel`
        if timeout is not None:
            end_time = timeout + time.monotonic()

        # Submit the applications
        app = self._get_app(fn)
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
        if cancel_futures:
            warn(message="Canceling on-going tasks is not supported in Parsl")
        if wait:
            self.dfk.wait_for_current_tasks()
        self.dfk.cleanup()
