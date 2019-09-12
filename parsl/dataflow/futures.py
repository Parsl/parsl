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

    The AppFuture will wait for the DFK to provide a result from an appropriate
    parent future, through `parent_callback`. It will set its result to the
    result of that parent future, if that parent future completes without an
    exception. This result setting should cause .result(), .exception() and done
    callbacks to fire as expected.

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

    def __init__(self, task_def):
        """Initialize the AppFuture.

        Args:

        KWargs:
             - task_def : The DFK task definition dictionary for the task represented
                   by this future.
        """
        super().__init__()
        self._update_lock = threading.Lock()
        self._outputs = []
        self.task_def = task_def

    def parent_callback(self, executor_fu):
        """Callback from a parent future to update the AppFuture.

        Used internally by AppFuture, and should not be called by code using AppFuture.

        Args:
            - executor_fu (Future): Future returned by the executor along with callback.

        Returns:
            - None

        Updates the future with the result() or exception()
        """
        with self._update_lock:

            if not executor_fu.done():
                raise ValueError("done callback called, despite future not reporting itself as done")

            try:
                res = executor_fu.result()
                if isinstance(res, RemoteExceptionWrapper):
                    res.reraise()
                self.set_result(executor_fu.result())

            except Exception as e:
                if executor_fu.retries_left > 0:
                    # ignore this exception, because assume some later
                    # parent executor, started external to this class,
                    # will provide the answer
                    pass
                else:
                    self.set_exception(e)

    @property
    def stdout(self):
        return self.task_def['kwargs'].get('stdout')

    @property
    def stderr(self):
        return self.task_def['kwargs'].get('stderr')

    @property
    def tid(self):
        return self.task_def['id']

    def cancel(self):
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self):
        return False

    @property
    def outputs(self):
        return self._outputs

    def __repr__(self):
        return '<%s super=%s>' % (
            self.__class__.__name__,
            super().__repr__())
