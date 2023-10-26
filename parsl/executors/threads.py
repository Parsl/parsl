import logging
import typeguard
import concurrent.futures as cf

from typing import List, Optional

from parsl.data_provider.staging import Staging
from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin
from parsl.executors.errors import UnsupportedFeatureError


logger = logging.getLogger(__name__)


class ThreadPoolExecutor(ParslExecutor, RepresentationMixin):
    """A thread-based executor.

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
        self.executor = cf.ThreadPoolExecutor(max_workers=self.max_threads,
                                              thread_name_prefix=self.thread_name_prefix)

    def submit(self, func, resource_specification, *args, **kwargs):
        """Submits work to the thread pool.

        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        """
        if resource_specification:
            logger.error("Ignoring the resource specification. "
                         "Parsl resource specification is not supported in ThreadPool Executor.")
            raise UnsupportedFeatureError('resource specification', 'ThreadPool Executor', None)

        return self.executor.submit(func, *args, **kwargs)

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
        x = self.executor.shutdown(wait=block)
        logger.debug("Done with executor shutdown")
        return x

    def monitor_resources(self):
        """Resource monitoring sometimes deadlocks when using threads, so this function
        returns false to disable it."""
        return False
