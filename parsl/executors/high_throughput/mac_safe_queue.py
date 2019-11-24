import multiprocessing
import multiprocessing.queues
import threading
import logging

logger = logging.getLogger(__name__)


class MacSafeQueue(multiprocessing.queues.Queue):
    """ Multiprocessing queues do not have qsize attributes on MacOS.
    This is slower but more portable version of the multiprocessing Queue
    that adds a explicit counter
    """

    def __init__(self, *args, **kwargs):
        if 'ctx' not in kwargs:
            kwargs['ctx'] = multiprocessing.get_context()
        super().__init__(*args, **kwargs)
        self._lock = threading.Lock()
        self._counter = 0

    def put(self, *args, **kwargs):
        logger.critical("Putting item {}".format(args))
        x = super().put(*args, **kwargs)
        with self._lock:
            self._counter += 1
        return x

    def get(self, *args, **kwargs):
        x = super().get(*args, **kwargs)
        with self._lock:
            self._counter -= 1
        logger.critical("Getting item {}".format(x))
        return x

    def qsize(self):
        return self._counter

    def empty(self):
        return not self._counter
