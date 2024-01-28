"""provides deferred/non-deep-stack callback execution.

it's not stack-safe to execute callbacks directly from another
callback, when those callbacks are involved in long dependency
chains: on my laptop a dependency chain of around 600 tasks,
when limited to a single core, can cause a recursion overflow.

this module is intended to address that by making callbacks on
task completion happen in a shallow stack: those callbacks never
return a result to their calling location (whch can happen in
two different places: either when adding the callback or when
setting a result, depending on the order of events, so there
isn't even a decent meaning for what returning a result (or
raising an exception) means there - exceptions are logged, and
they can be just as well be logged from a shallow thread)

this also pulls callback execution out of the various threads
that executors might have for processing results. I think thats
a vaguely good thing.

These callbacks should be scoped by DFK, I think? rather than a
global thread.
"""

import logging

from queue import SimpleQueue
from threading import Thread

from parsl.process_loggers import wrap_with_logs

logger = logging.getLogger(__name__)


class DeferredCallbackManager:
    def __init__(self):
        self._queue = SimpleQueue()  # can this overflow like a stack or is it more heap constrained which allows it to grow bigger?
        self._thread = Thread(target=self._deferred_callback_thread, name="Deferred-Callback-Manager", daemon=True)
        self._thread.start()

    def defer(self, func):
        def cb(*args, **kwargs):
            logger.debug(f"Putting callback {func} into queue with args {args}")
            self._queue.put((func, args, kwargs))
            # queues this call:
            #    func(*args, **kwargs)
            # to happen later
        return cb

    @wrap_with_logs
    def _deferred_callback_thread(self):
        # TODO: this needs to shutdown with the DFK... which is going to need awkward polling behaviour?

        while True:
            logger.debug("Waiting for a callback")
            (func, args, kwargs) = self._queue.get()
            logger.debug(f"Pulled callback {func} from queue with args {args}")
            # TODO: error handling
            try:
                func(*args, **kwargs)
            except Exception:
                logger.error("deferred callback got an exception which will be ignored", exc_info=True)
