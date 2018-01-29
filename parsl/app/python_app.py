import logging

from parsl.app.futures import DataFuture
from parsl.app.app import AppBase

logger = logging.getLogger(__name__)


class PythonApp(AppBase):
    """ Extends AppBase to cover the Python App

    """

    def __init__(self, func, executor, walltime=60, cache=False,
                 sites='all', fn_hash=None):
        ''' Initialize the super. This bit is the same for both bash & python apps.
        '''
        super().__init__(func, executor, walltime=walltime, sites=sites, exec_type="python")
        self.fn_hash = fn_hash
        self.cache = cache

    def __call__(self, *args, **kwargs):
        ''' This is where the call to a python app is handled

        Args:
             - Arbitrary
        Kwargs:
             - Arbitrary

        Returns:
             If outputs=[...] was a kwarg then:
                   App_fut, [Data_Futures...]
             else:
                   App_fut

        '''
        app_fut = self.executor.submit(self.func, *args,
                                       parsl_sites=self.sites,
                                       fn_hash=self.fn_hash,
                                       cache=self.cache,
                                       **kwargs)

        logger.debug("App[%s] assigned Task_id:[%s]" % (self.func.__name__,
                                                        app_fut.tid))
        out_futs = [DataFuture(app_fut, o, parent=app_fut, tid=app_fut.tid)
                    for o in kwargs.get('outputs', [])]
        app_fut._outputs = out_futs

        return app_fut
