from __future__ import annotations

import logging
import threading
from concurrent.futures import Future
from typing import Any, Optional, Sequence, Union

import parsl.app.app as app
from parsl.app.futures import DataFuture
from parsl.dataflow.taskrecord import TaskRecord

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
    parent future, through ``parent_callback``. It will set its result to the
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

    def __init__(self, task_record: TaskRecord) -> None:
        """Initialize the AppFuture.

        Args:

        KWargs:
             - task_record : The TaskRecord for the task represented by
               this future.
        """
        super().__init__()
        self._update_lock = threading.Lock()
        self._outputs: Sequence[DataFuture]
        self._outputs = []
        self.task_record = task_record

        self._stdout_future: Optional[DataFuture] = None
        self._stderr_future: Optional[DataFuture] = None

    @property
    def stdout(self) -> Union[None, str, DataFuture]:
        """Return app stdout. If stdout was specified as a string, then this
        property will return that string. If stdout was specified as a File,
        then this property will return a DataFuture representing that file
        stageout.
        TODO: this can be a tuple too I think?"""
        if self._stdout_future:
            return self._stdout_future
        else:
            # this covers the str and None cases
            return self.task_record['kwargs'].get('stdout')

    @property
    def stderr(self) -> Union[None, str, DataFuture]:
        """Return app stderr. If stdout was specified as a string, then this
        property will return that string. If stdout was specified as a File,
        then this property will return a DataFuture representing that file
        stageout.
        TODO: this can be a tuple too I think?"""
        if self._stderr_future:
            return self._stderr_future
        else:
            # this covers the str and None cases
            return self.task_record['kwargs'].get('stderr')

    @property
    def tid(self) -> int:
        return self.task_record['id']

    def cancel(self) -> bool:
        raise NotImplementedError("Cancel not implemented")

    def cancelled(self) -> bool:
        return False

    def task_status(self) -> str:
        """Returns the status of the task that will provide the value
           for this future.  This may not be in-sync with the result state
           of this future - for example, task_status might return 'done' but
           self.done() might not be true (which in turn means self.result()
           and self.exception() might block).

           The actual status description strings returned by this method are
           likely to change over subsequent versions of parsl, as use-cases
           and infrastructure are worked out.

           It is expected that the status values will be from a limited set
           of strings (so that it makes sense, for example, to group and
           count statuses from many futures).

           It is expected that might be a non-trivial cost in acquiring the
           status in future (for example, by communicating with a remote
           worker).

           Returns: str
        """
        return self.task_record['status'].name

    @property
    def outputs(self) -> Sequence[DataFuture]:
        return self._outputs

    def __getitem__(self, key: Any) -> AppFuture:
        # This is decorated on each invocation because the getitem task
        # should be bound to the same DFK as the task associated with this
        # Future.
        deferred_getitem_app = app.python_app(deferred_getitem, executors=['_parsl_internal'], data_flow_kernel=self.task_record['dfk'])

        return deferred_getitem_app(self, key)

    def __getattr__(self, name: str) -> AppFuture:
        # this will avoid lifting behaviour on private methods and attributes,
        # including __double_underscore__ methods which implement other
        # Python syntax (such as iterators in for loops)
        if name.startswith("_"):
            raise AttributeError()

        deferred_getattr_app = app.python_app(deferred_getattr, executors=['_parsl_internal'], data_flow_kernel=self.task_record['dfk'])

        return deferred_getattr_app(self, name)


def deferred_getitem(o: Any, k: Any) -> Any:
    return o[k]


def deferred_getattr(o: Any, name: str) -> Any:
    return getattr(o, name)
