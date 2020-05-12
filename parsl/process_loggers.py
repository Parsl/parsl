import logging
import threading
import functools

from typing import cast, Callable, TypeVar

logger = logging.getLogger(__name__)

Signature = TypeVar('Signature', bound=Callable)


def wrap_with_logs(fn: Signature) -> Signature:
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        thread = threading.current_thread()

        name = "thread {} in process {}".format(thread.name, "UNLOGGED")

        try:
            r = fn(*args, **kwargs)
            logger.info("exception wrapper: normal ending for {}".format(name))
            return r
        except Exception:
            logger.error("exception wrapper: exceptional ending for {}".format(name))
            logger.exception("exception wrapper: caught an exception for {}".format(name))
            raise
    # This cast asserts without checking that the wrapped function has the same signature
    # as the supplied 'fn' function
    return cast(Signature, wrapped)


def hanging_wrap_with_logs(fn):
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        # logger.info("exception wrapper: start")
        # logger.debug("exception wrapper: about to get current process")

        # process = multiprocessing.current_process()
        # logger.debug("got current process, about to get thread")
        thread = threading.current_thread()
        # logger.debug("got thread, about to form name")

        name = "thread {}".format(thread.name)

        try:
            logger.info("exception wrapper: started for {}".format(name))
            r = fn(*args, **kwargs)
            logger.info("exception wrapper: normal ending for {}".format(name))
            return r
        except Exception:
            logger.error("exception wrapper: exceptional ending for {}".format(name))
            logger.exception("exception wrapper: caught an exception for {}".format(name))
            raise
    return wrapped
