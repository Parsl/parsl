import concurrent.futures as cf
import logging
from typing import List, Optional

import typeguard

from parsl.data_provider.staging import Staging
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class ImmediateExecutor(ParslExecutor, RepresentationMixin):
    """ -- forked from thread pool executor...

    Parameters
    ----------
    max_threads : Optional[int]
        Number of threads. Default is 2.
    thread_name_prefix : string
        Thread name prefix
    storage_access : list of :class:`~parsl.data_provider.staging.Staging`
        Specifications for accessing data this executor remotely.
    """

    @typeguard.typechecked
    def __init__(self, label: str = 'threads', max_threads: Optional[int] = 2,
                 thread_name_prefix: str = '', storage_access: Optional[List[Staging]] = None,
                 working_dir: Optional[str] = None):
        ParslExecutor.__init__(self)
        self.label = label
        self.max_threads = max_threads
        self.thread_name_prefix = thread_name_prefix

        # we allow storage_access to be None now, which means something else to [] now
        # None now means that a default storage access list will be used, while
        # [] is a list with no storage access in it at all
        self.storage_access = storage_access
        self.working_dir = working_dir

    def start(self):
        pass

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the thread pool.

        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        """
        future = cf.Future()
        future.set_result(func(*args, **kwargs))
        return future

    def shutdown(self, block=True):
        """Shutdown the ThreadPool. The underlying concurrent.futures thread pool
        implementation will not terminate tasks that are being executed, because it
        does not provide a mechanism to do that. With block set to false, this will
        return immediately and it will appear as if the DFK is shut down, but
        the python process will not be able to exit until the thread pool has
        emptied out by task completions. In either case, this can be a very long wait.

        Kwargs:
            - block (Bool): To block for confirmations or not

        """
        logger.debug("Shutting down executor, which involves waiting for running tasks to complete")
        logger.debug("Done with executor shutdown")

    def monitor_resources(self):
        """Resource monitoring sometimes deadlocks when using threads, so this function
        returns false to disable it."""
        return False
