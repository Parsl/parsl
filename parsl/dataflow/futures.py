"""This module implements the AppFutures.

We have two basic types of futures:
    1. DataFutures which represent data objects
    2. AppFutures which represent the futures on App/Leaf tasks.

"""

from concurrent.futures import Future
import logging
import threading

from parsl.app.errors import RemoteExceptionWrapper

logger = logging.getLogger(__name__)

# Possible future states (for internal use by the futures package).
PENDING = 'PENDING'
RUNNING = 'RUNNING'
# The future was cancelled by the user...
CANCELLED = 'CANCELLED'
# ...and _Waiter.add_cancelled() was called by a worker.
CANCELLED_AND_NOTIFIED = 'CANCELLED_AND_NOTIFIED'
FINISHED = 'FINISHED'

_STATE_TO_DESCRIPTION_MAP = {
    PENDING: "pending",
    RUNNING: "running",
    CANCELLED: "cancelled",
    CANCELLED_AND_NOTIFIED: "cancelled",
    FINISHED: "finished"
}


class AppFuture(Future):
    """An AppFuture wraps a sequence of Futures which may fail and be retried.

    An AppFuture starts with no parent future. A sequence of parent futures may
    be assigned by code outside of this class, by passing that new parent future
    into "update_future".

    The AppFuture will set its result to the result of the parent future, if that
    parent future completes without an exception. This result setting should
    cause .result(), .exception() and done callbacks to fire as expected when a
    Future has a result set.

    The AppFuture will not set its result to the result of the parent future, if
    that parent future completes with an exception, and if that parent future
    has retries left. In that case, no result(), exception() or done callbacks should
    report a result.

    The AppFuture will set its result to the result of the parent future, if that
    parent future completes with an exception and if that parent future has no
    retries left, or if it has no retry field. .result(), .exception() and done callbacks
    should give a result as expected when a Future has a result set

    The parent future may return a RemoteExceptionWrapper as a result
    and AppFuture will treat this an an exception for the above
    retry and result handling behaviour.

    """

    def __init__(self, tid=None, stdout=None, stderr=None):
        """Initialize the AppFuture.

        Args:

        KWargs:
             - tid (Int) : Task id should be any unique identifier. Now Int.
             - stdout (str) : Stdout file of the app.
                   Default: None
             - stderr (str) : Stderr file of the app.
                   Default: None
        """
        self._tid = tid
        super().__init__()
        self.parent = None
        self._update_lock = threading.Lock()
        self._outputs = []
        self._stdout = stdout
        self._stderr = stderr

    def parent_callback(self, executor_fu):
        """Callback from a parent future to update the AppFuture.

        Used internally by AppFuture, and should not be called by code using AppFuture.

        Args:
            - executor_fu (Future): Future returned by the executor along with callback.
              This may not be the current parent future, as the parent future may have
              already been updated to point to a retrying execution, and in that case,
              this is logged.

              In the case that a new parent has been attached, we must immediately discard
              this result no matter what it contains (although it might be interesting
              to log if it was successful...)

        Returns:
            - None

        Updates the super() with the result() or exception()
        """
        with self._update_lock:

            if not executor_fu.done():
                raise ValueError("done callback called, despite future not reporting itself as done")

            # this is for consistency checking
            if executor_fu != self.parent:
                if executor_fu.exception() is None and not isinstance(executor_fu.result(), RemoteExceptionWrapper):
                    # ... then we completed with a value, not an exception or wrapped exception,
                    # but we've got an updated executor future.
                    # This is bad - for example, we've started a retry even though we have a result

                    raise ValueError("internal consistency error: AppFuture done callback called without an exception, but parent has been changed since then")

            try:
                res = executor_fu.result()
                if isinstance(res, RemoteExceptionWrapper):
                    res.reraise()
                super().set_result(executor_fu.result())

            except Exception as e:
                if executor_fu.retries_left > 0:
                    # ignore this exception, because assume some later
                    # parent executor, started external to this class,
                    # will provide the answer
                    pass
                else:
                    super().set_exception(e)

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    @property
    def tid(self):
        return self._tid

    def update_parent(self, fut):
        """Add a callback to the parent to update the state.

        This handles the case where the user has called result on the AppFuture
        before the parent exists.
        """
        self.parent = fut

        try:
            fut.add_done_callback(self.parent_callback)
        except Exception as e:
            logger.error("add_done_callback got an exception {} which will be ignored".format(e))

    def cancel(self):
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self):
        return False

    def running(self):
        if self.parent:
            return self.parent.running()
        else:
            return False

    @property
    def outputs(self):
        return self._outputs

    def __repr__(self):
        return '<%s super=%s parent=%s>' % (
            self.__class__.__name__,
            super().__repr__(),
            self.parent.__repr__())
