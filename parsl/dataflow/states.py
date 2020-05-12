from enum import IntEnum


class States(IntEnum):
    """Map states for tasks to an int."""
    unsched = -1
    pending = 0

    running = 2
    # this state is special - a task record never goes into
    # this state, but monitoring may represent a task in
    # this state because the monitoring DB contains different information

    done = 3
    exec_done = 3
    failed = 4
    dep_fail = 5
    launched = 7
    memo_done = 8
    fail_retryable = 9


# states from which we will never move to another state
FINAL_STATES = [States.exec_done, States.memo_done, States.failed, States.dep_fail]

# states which are final and which indicate a failure. This must
# be a subset of FINAL_STATES
FINAL_FAILURE_STATES = [States.failed, States.dep_fail]
