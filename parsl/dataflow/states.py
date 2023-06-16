from enum import IntEnum


class States(IntEnum):
    """Enumerates the states a parsl task may be in.

    These states occur inside the task record for a task inside
    a `DataFlowKernel` and in the monitoring database.

    In a single successful task execution, tasks will progress in this
    sequence:

    pending -> launched -> running -> running_ended -> exec_done

    Other states represent deviations from this path, either due to
    failures, or to deliberate changes to how tasks are executed (for
    example due to join_app, or memoization).


    All tasks should end up in one of the states listed in `FINAL_STATES`.
    """

    unsched = -1
    pending = 0
    """Task is known to parsl but cannot run yet. Usually, a task cannot
    run because it is waiting for dependency tasks to complete.
    """

    running = 2
    """Task is running on a resource. This state is special - a DFK task
    record never goes to States.running state; but the monitoring database
    may represent a task in this state based on non-DFK information received
    from monitor_wrapper."""

    exec_done = 3
    """Task has been executed successfully."""

    failed = 4
    """Task has failed and no more attempts will be made to run it."""

    dep_fail = 5
    """Dependencies of this task failed, so it is marked as failed without
    even an attempt to launch it."""

    launched = 7
    """Task has been passed to a `ParslExecutor` for execution."""

    fail_retryable = 8
    """Task has failed, but can be retried"""

    memo_done = 9
    """Task was found in the memoization table, so it is marked as done
    without even an attempt to launch it."""

    joining = 10
    """Task is a join_app, joining on internal tasks. The task has run its
    own Python code, and is now waiting on other tasks before it can make
    further progress (to a done/failed state)."""

    running_ended = 11
    """Like States.running, this state is also not observed by the DFK,
    but instead only by monitoring. This state does not record
    anything about task success or failure, merely that the wrapper
    ran long enough to record it as finished."""

    def __str__(self) -> str:
        return self.__class__.__name__ + "." + self.name


FINAL_STATES = [States.exec_done, States.memo_done, States.failed, States.dep_fail]
"""States from which we will never move to another state, because the job has
either definitively completed or failed."""

FINAL_FAILURE_STATES = [States.failed, States.dep_fail]
"""States which are final and which indicate a failure. This must
be a subset of FINAL_STATES"""
