import logging
import sys
import concurrent.futures as cf
from parsl.executors.base import ParslExecutor

logger = logging.getLogger(__name__)

class ThreadPoolExecutor(ParslExecutor):
    ''' The thread pool executor
    '''

    def __init__ (self, max_workers=2, thread_name_prefix='',
                  execution_provider=None, config=None):
        ''' Initialize the thread pool
        Config options that are really used are :

        config.sites.site.execution.options = {"maxThreads" : <int>,
                                               "threadNamePrefix" : <string>}

        Kwargs:
           - max_workers (int) : Number of threads (Default=2) (keeping name workers/threads for backward compatibility)
           - thread_name_prefix (string) : Thread name prefix (Only supported in python v3.6+
           - execution_provider (ep object) : This is ignored here
           - config (dict): The config dict object for the site:


        '''
        self._scaling_enabled = False
        if not config :
            config = {"execution" : { } }
        if "maxThreads" not in config["execution"]:
            config["execution"]["maxThreads"] = max_workers
        if "threadNamePrefix" not in config["execution"]:
            config["execution"]["threadNamePrefix"] = thread_name_prefix

        self.config = config

        if sys.version_info > (3,6):
            self.executor = cf.ThreadPoolExecutor(max_workers=config["execution"]["maxThreads"],
                                                  thread_name_prefix=config["execution"]["threadNamePrefix"])
        else:
            self.executor = cf.ThreadPoolExecutor(max_workers=config["execution"]["maxThreads"])


    @property
    def scaling_enabled(self):
        return self._scaling_enabled

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

