import logging
import threading

import tblib.pickling_support

tblib.pickling_support.install()

from functools import wraps

from parsl.app.app import AppBase
from parsl.app.errors import wrap_error
from parsl.dataflow.dflow import DataFlowKernelLoader
from parsl.utils import AutoCancelTimer

logger = logging.getLogger(__name__)


def timeout(f, seconds: float):
    @wraps(f)
    def wrapper(*args, **kwargs):
        import ctypes

        import parsl.app.errors

        def inject_exception(thread):
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread),
                ctypes.py_object(parsl.app.errors.AppTimeout)
            )

        thread = threading.current_thread().ident
        with AutoCancelTimer(seconds, inject_exception, args=[thread]):
            return f(*args, **kwargs)
    return wrapper


class PythonApp(AppBase):
    """Extends AppBase to cover the Python App."""

    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=None, join=False):
        super().__init__(
            wrap_error(func),
            data_flow_kernel=data_flow_kernel,
            executors=executors,
            cache=cache,
            ignore_for_cache=ignore_for_cache
        )
        self.join = join

    def __call__(self, *args, **kwargs):
        """This is where the call to a python app is handled.

        Args:
             - Arbitrary
        Kwargs:
             - Arbitrary

        Returns:
                   App_fut

        """
        invocation_kwargs = {}
        invocation_kwargs.update(self.kwargs)
        invocation_kwargs.update(kwargs)

        if self.data_flow_kernel is None:
            dfk = DataFlowKernelLoader.dfk()
        else:
            dfk = self.data_flow_kernel

        walltime = invocation_kwargs.get('walltime')
        if walltime is not None:
            func = timeout(self.func, walltime)
        else:
            func = self.func

        app_fut = dfk.submit(func, app_args=args,
                             executors=self.executors,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=invocation_kwargs,
                             join=self.join)

        return app_fut
