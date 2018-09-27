import logging

import tblib.pickling_support
tblib.pickling_support.install()

from parsl.app.futures import DataFuture
from parsl.app.app import AppBase
from parsl.app.errors import wrap_error
from parsl.dataflow.dflow import DataFlowKernelLoader


logger = logging.getLogger(__name__)


class PythonApp(AppBase):
    """Extends AppBase to cover the Python App."""

    def __init__(self, func, data_flow_kernel=None, walltime=60, cache=False, executors='all'):
        super().__init__(
            wrap_error(func),
            data_flow_kernel=data_flow_kernel,
            walltime=walltime,
            executors=executors,
            cache=cache
        )

    def __call__(self, *args, **kwargs):
        """This is where the call to a python app is handled.

        Args:
             - Arbitrary
        Kwargs:
             - Arbitrary

        Returns:
             If outputs=[...] was a kwarg then:
                   App_fut, [Data_Futures...]
             else:
                   App_fut

        """
        if self.data_flow_kernel is None:
            self.data_flow_kernel = DataFlowKernelLoader.dfk()
        app_fut = self.data_flow_kernel.submit(self.func, *args,
                                               executors=self.executors,
                                               fn_hash=self.func_hash,
                                               cache=self.cache,
                                               **kwargs)

        # logger.debug("App[{}] assigned Task[{}]".format(self.func.__name__,
        #                                                 app_fut.tid))
        out_futs = [DataFuture(app_fut, o, parent=app_fut, tid=app_fut.tid)
                    for o in kwargs.get('outputs', [])]
        app_fut._outputs = out_futs

        return app_fut
