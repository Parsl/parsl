import logging
import sys
import typeguard
import concurrent.futures as cf

from typing import Any, List, Optional

from parsl.executors.base import ParslExecutor
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)


class ThreadPoolExecutor(ParslExecutor, RepresentationMixin):
    """A thread-based executor.

    Parameters
    ----------
    max_threads : int
        Number of threads. Default is 2.
    thread_name_prefix : string
        Thread name prefix (only supported in python v3.6+).
    storage_access : list of :class:`~parsl.data_provider.staging.Staging`
        Specifications for accessing data this executor remotely.
    managed : bool
        If True, parsl will control dynamic scaling of this executor, and be responsible. Otherwise,
        this is managed by the user.
    """

    @typeguard.typechecked
    def __init__(self, label: str = 'threads', max_threads: int = 2,
                 thread_name_prefix: str = '', storage_access: List[Any] = None,
                 working_dir: Optional[str] = None, managed: bool = True):
        self.label = label
        self._scaling_enabled = False
        self.max_threads = max_threads
        self.thread_name_prefix = thread_name_prefix

        # we allow storage_access to be None now, which means something else to [] now
        # None now means that a default storage access list will be used, while
        # [] is a list with no storage access in it at all
        self.storage_access = storage_access
        self.working_dir = working_dir
        self.managed = managed

    def start(self):
        if sys.version_info > (3, 6):
            self.executor = cf.ThreadPoolExecutor(max_workers=self.max_threads,
                                                  thread_name_prefix=self.thread_name_prefix)
        else:
            self.executor = cf.ThreadPoolExecutor(max_workers=self.max_threads)

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def submit(self, *args, **kwargs):
        """Submits work to the thread pool.

        This method is simply pass through and behaves like a submit call as described
        here `Python docs: <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_

        """
        return self.executor.submit(*args, **kwargs)

    def scale_out(self, workers=1):
        """Scales out the number of active workers by 1.

        This method is notImplemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        """

        raise NotImplementedError

    def scale_in(self, blocks):
        """Scale in the number of active blocks by specified amount.

        This method is not implemented for threads and will raise the error if called.

        Raises:
             NotImplemented exception
        """

        raise NotImplementedError

    def shutdown(self, block=False):
        """Shutdown the ThreadPool.

        Kwargs:
            - block (Bool): To block for confirmations or not

        """
        x = self.executor.shutdown(wait=block)
        logger.debug("Done with executor shutdown")
        return x
