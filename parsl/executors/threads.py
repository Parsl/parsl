import concurrent.futures
import logging
from parsl.executors.base import ParslExecutor

logger = logging.getLogger(__name__)

class ThreadPoolExecutor(ParslExecutor):
    ''' The thread pool executor
    '''

    def __init__ (self, max_workers=2, thread_name_prefix=''):
        ''' Initialize the thread pool
        '''
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)


    def submit (self, *args, **kwargs):
        ''' Submits work to the thread pool
        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        Returns:
              Future
        '''

        return self.executor.submit(*args, **kwargs)

    def scale_out (self, workers=1):
        ''' Scales out the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''

        raise NotImplemented

    def scale_in (self, workers=1):
        ''' Scale in the number of active workers by 1
        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        '''

        raise NotImplemented

