import logging
from ipyparallel import Client
from parsl.executors.base import ParslExecutor

logger = logging.getLogger(__name__)

class IPyParallelExecutor(ParslExecutor):
    ''' The Ipython parallel executor
    '''

    def __init__ (self, execution_provider=None):
        ''' Initialize the thread pool
        '''
        self.executor = Client()
        if execution_provider:
            self._scaling_enabled = True
        else:
            self._scaling_enabled = False

        self.execution_provider = execution_provider
        self.lb_view  = self.executor.load_balanced_view()
        logger.debug("Starting executor")

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def submit (self,  *args, **kwargs):
        ''' Submits work to the thread pool
        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Returns:
              Future
        '''
        logger.debug("Got args : %s,", args)
        logger.debug("Got kwargs : %s,", kwargs)
        return self.lb_view.apply_async(*args, **kwargs)

    def scale_out (self, *args, **kwargs):
        ''' Scales out the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''
        if self.execution_provider :
            r = self.execution_provider.scale_out(*args, **kwargs)
        else:
            logger.error("No execution provider available")
            r = None

        return r

    def scale_in (self, workers=1):
        ''' Scale in the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''
        if self.execution_provider :
            r = self.execution_provider.scale_in(*args, **kwargs)
        else:
            logger.error("No execution provider available")
            r = None

        return r


if __name__ == "__main__" :

    pool1_config = {"poolname" : "pool1",
                    "queue"    : "foo" }
