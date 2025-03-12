"""Helpers for cross-platform multiprocessing support.
"""

import logging
import multiprocessing
import multiprocessing.queues
import platform
from multiprocessing.context import ForkProcess as ForkProcessType
from typing import Callable

logger = logging.getLogger(__name__)

ForkContext = multiprocessing.get_context("fork")
SpawnContext = multiprocessing.get_context("spawn")

ForkProcess: Callable[..., ForkProcessType] = ForkContext.Process


class MacSafeQueue(multiprocessing.queues.Queue):
    """ Multiprocessing queues do not have qsize attributes on MacOS.
    This is slower but more portable version of the multiprocessing Queue
    that adds a explicit counter

    Reference : https://github.com/keras-team/autokeras/commit/4ddd568b06b4045ace777bc0fb7bc18573b85a75
    """

    def __init__(self, *args, **kwargs):
        if 'ctx' not in kwargs:
            kwargs['ctx'] = multiprocessing.get_context()
        super().__init__(*args, **kwargs)
        self._counter = multiprocessing.Value('i', 0)

    def put(self, *args, **kwargs):
        # logger.critical("Putting item {}".format(args))
        x = super().put(*args, **kwargs)
        with self._counter.get_lock():
            self._counter.value += 1
        return x

    def get(self, *args, **kwargs):
        x = super().get(*args, **kwargs)
        with self._counter.get_lock():
            self._counter.value -= 1
        # logger.critical("Getting item {}".format(x))
        return x

    def qsize(self):
        return self._counter.value

    def empty(self):
        return not self._counter.value


# SizedQueue should be constructable using the same calling
# convention as multiprocessing.Queue but that entire signature
# isn't expressible in mypy 0.790
SizedQueue: Callable[..., multiprocessing.Queue]


if platform.system() != 'Darwin':
    import multiprocessing
    SizedQueue = multiprocessing.Queue
else:
    SizedQueue = MacSafeQueue
