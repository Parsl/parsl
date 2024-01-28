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

logger = logging.getLogger(__name__)


class DeferredCallbackManager:
    def __init__(self):
        self.queue = []  # TODO: some threading queue would be better

    def defer(self, func):
        def cb(*args, **kwargs):
            # TODO error handling
            func(*args, **kwargs)  # fake-deferral
        return cb
