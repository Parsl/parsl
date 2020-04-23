import logging

import tblib.pickling_support
tblib.pickling_support.install()

from parsl.app.app import AppBase
from parsl.app.errors import wrap_error
from parsl.dataflow.dflow import DataFlowKernelLoader


logger = logging.getLogger(__name__)


def timeout(f, seconds):
    def wrapper(*args, **kwargs):
        import threading
        import ctypes
        import parsl.app.errors

        def inject_exception(thread):
            ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_long(thread),
                ctypes.py_object(parsl.app.errors.AppTimeout)
            )

        thread = threading.current_thread().ident
        timer = threading.Timer(seconds, inject_exception, args=[thread])
        timer.start()
        result = f(*args, **kwargs)
        timer.cancel()
        return result
    return wrapper


class PythonApp(AppBase):
    """Extends AppBase to cover the Python App."""

    def __init__(self, func, data_flow_kernel=None, cache=False, executors='all', ignore_for_cache=[]):
        super().__init__(
            wrap_error(func),
            data_flow_kernel=data_flow_kernel,
            executors=executors,
            cache=cache,
            ignore_for_cache=ignore_for_cache
        )

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
                             fn_hash=self.func_hash,
                             cache=self.cache,
                             ignore_for_cache=self.ignore_for_cache,
                             app_kwargs=kwargs)

        return app_fut
