"""Helpers for cross-platform multiprocessing support.
"""

import logging
import multiprocessing
import multiprocessing.queues
import platform

logger = logging.getLogger(__name__)

ForkContext = multiprocessing.get_context("fork")
SpawnContext = multiprocessing.get_context("spawn")

# maybe ForkProcess should be: Callable[..., Process] so as to make
# it clear that it returns a Process always to the type checker?
ForkProcess = multiprocessing.context.ForkProcess
SpawnProcess = multiprocessing.context.SpawnProcess


def forkProcess(*args, **kwargs) -> ForkProcess:
    P = multiprocessing.get_context('fork').Process
    # reveal_type(P)
    return P(*args, **kwargs)


def spawnProcess(*args, **kwargs) -> SpawnProcess:
    P = multiprocessing.get_context('spawn').Process
    # reveal_type(P)
    return P(*args, **kwargs)


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
# SizedQueue: Callable[..., multiprocessing.Queue]

def sizedQueue(*args, **kwargs) -> multiprocessing.queues.Queue:
    if platform.system() != 'Darwin':
        return multiprocessing.Queue(*args, **kwargs)
    else:
        return MacSafeQueue(*args, **kwargs)
