import functools
import logging
import threading
import uuid
from typing import Callable, Optional


def wrap_with_logs(fn: Optional[Callable] = None, target: str = __name__) -> Callable:
    """Calls the supplied function, and logs whether that
    function raised an exception or terminated normally.

    This is intended to be used around the top level functions of
    processes and threads, where exceptions would normally not
    go to a log.
    """

    def decorator(func):

        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            assert func is not None
            uid = uuid.uuid4()
            thread = threading.current_thread()
            name = f"{func.__name__} on thread {thread.name}"
            logger = logging.getLogger(target)

            try:
                logger.debug("Starting %s", name, extra={"wrap_id": uid})
                r = func(*args, **kwargs)
                logger.debug("Normal ending for %s", name, extra={"wrap_id": uid})
                return r
            except Exception:
                logger.error("Exceptional ending for %s", name, exc_info=True, extra={"wrap_id": uid})
                raise

        return wrapped

    if fn is not None:
        return decorator(fn)
    else:
        return decorator
