from __future__ import annotations

import threading
import datetime
from typing_extensions import TypedDict
from concurrent.futures import Future

# only for type checking:
from typing import Any, Callable, Dict, Optional, List, Sequence, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from parsl.dataflow.futures import AppFuture

from parsl.dataflow.states import States


class TaskRecord(TypedDict, total=False):
    """This stores most information about a Parsl task"""

    func_name: str

    status: States

    depends: List[Future]

    app_fu: AppFuture
    """The Future which was returned to the user when an app was invoked.
    """

    exec_fu: Optional[Future]
    """When a task has been launched on an executor, stores the Future
    returned by that executor.
    """

    executor: str
    """The name of the executor which this task will be/is being/was
    executed on.
    """

    retries_left: int
    fail_count: int
    fail_cost: float
    fail_history: List[str]

    checkpoint: bool  # this change is also in #1516
    """Should this task be checkpointed?
    """

    hashsum: Optional[str]  # hash for checkpointing/memoization.
    """The hash used for checkpointing and memoisation. This is not known
    until at least all relevant dependencies have completed, and will be
    None before that.
    """

    task_launch_lock: threading.Lock
    """This lock is used to ensure that task launch only happens once.
    A task can be launched by dependencies completing from arbitrary
    threads, and a race condition would exist when dependencies complete
    in multiple threads very close together in time, which this lock
    prevents.
    """

    # these three could be more strongly typed perhaps but I'm not thinking about that now
    func: Callable
    fn_hash: str
    args: Sequence[Any]  # in some places we uses a Tuple[Any, ...] and in some places a List[Any]. This is an attempt to correctly type both of those.
    kwargs: Dict[str, Any]

    time_invoked: Optional[datetime.datetime]
    time_returned: Optional[datetime.datetime]
    try_time_launched: Optional[datetime.datetime]
    try_time_returned: Optional[datetime.datetime]

    memoize: bool
    """Should this task be memoized?"""
    ignore_for_cache: Sequence[str]
    from_memo: Optional[bool]

    id: int
    try_id: int

    resource_specification: Dict[str, Any]
    """Dictionary containing relevant info for a task execution.
    Includes resources to allocate and execution mode as a given
    executor permits."""

    join: bool
    """Is this a join_app?"""

    joins: Union[None, Future, List[Future]]
    """If this is a join app and the python body has executed, then this
    contains the Future or list of Futures that the join app will join."""

    join_lock: threading.Lock
    """Restricts access to end-of-join behavior to ensure that joins
    only complete once, even if several joining Futures complete close
    together in time."""
