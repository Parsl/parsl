"""Interfaces modeled after Python's `concurrent library <https://docs.python.org/3/library/concurrent.html>`_"""
from concurrent.futures import Executor
from typing import Callable

from parsl import Config, DataFlowKernel
from parsl.app.python import PythonApp


class ParslPoolExecutor(Executor):
    """An executor that uses a pool of workers managed by Parsl"""

    def __init__(self, config: Config):
        """Create the executor

        Args:
            config: Configuration for the Parsl Data Flow Kernel (DFK)
        """
        self._config = config
        self.dfk = DataFlowKernel(self._config)
        self._app_cache = {}  # Cache specific to this instance: https://stackoverflow.com/questions/33672412

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

    def shutdown(self, wait: bool = ..., *, cancel_futures: bool = ...) -> None:
        if wait:
            self.dfk.wait_for_current_tasks()
        self.dfk.cleanup()
