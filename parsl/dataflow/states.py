from enum import IntEnum


class States(IntEnum):
    """Map states for tasks to an int."""
    unsched = -1
    pending = 0
    running = 2
    done = 3
    failed = 4
    dep_fail = 5
    launched = 7


# states from which we will never move to another state
FINAL_STATES = [States.done, States.failed, States.dep_fail]

# states which are final and which indicate a failure. This must
# be a subset of FINAL_STATES
FINAL_FAILURE_STATES = [States.failed, States.dep_fail]
