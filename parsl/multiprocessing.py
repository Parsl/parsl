"""Helpers for cross-platform multiprocessing support.
"""

import logging
import multiprocessing
import multiprocessing.queues
import platform
from multiprocessing.context import ForkProcess as ForkProcessType
from multiprocessing.context import SpawnProcess as SpawnProcessType
from typing import Callable

logger = logging.getLogger(__name__)

ForkContext = multiprocessing.get_context("fork")
SpawnContext = multiprocessing.get_context("spawn")

ForkProcess: Callable[..., ForkProcessType] = ForkContext.Process
SpawnProcess: Callable[..., SpawnProcessType] = SpawnContext.Process

SpawnEvent = SpawnContext.Event
SpawnQueue = SpawnContext.Queue


class MacSafeQueue(multiprocessing.queues.Queue):
    """ Multiprocessing queues do not have qsize attributes on MacOS.
    This is slower but more portable version of the multiprocessing Queue
    that adds a explicit counter

    Reference : https://github.com/keras-team/autokeras/commit/4ddd568b06b4045ace777bc0fb7bc18573b85a75
    """

    def __init__(self, *args, **kwargs):
        if 'ctx' not in kwargs:
            kwargs['ctx'] = multiprocessing.get_context('spawn')
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
    SizedQueue = SpawnQueue
else:
    SizedQueue = MacSafeQueue


def join_terminate_close_proc(process: SpawnProcessType, *, timeout: int = 30) -> None:
    """Increasingly aggressively terminate a process.

    This function assumes that the process is likely to exit before
    the join timeout, driven by some other means, such as the
    MonitoringHub router_exit_event. If the process does not exit, then
    first terminate() and then kill() will be used to end the process.

    In the case of a very mis-behaving process, this function might take
    up to 3*timeout to exhaust all termination methods and return.
    """
    logger.debug("Joining process")
    process.join(timeout)

    # run a sequence of increasingly aggressive steps to shut down the process.
    if process.is_alive():
        logger.error("Process did not join. Terminating.")
        process.terminate()
        process.join(timeout)
        if process.is_alive():
            logger.error("Process did not join after terminate. Killing.")
            process.kill()
            process.join(timeout)
            # This kill should not be caught by any signal handlers so it is
            # unlikely that this join will timeout. If it does, there isn't
            # anything further to do except log an error in the next if-block.

    if process.is_alive():
        logger.error("Process failed to end")
        # don't call close if the process hasn't ended:
        # process.close() doesn't work on a running process.
    else:
        process.close()
