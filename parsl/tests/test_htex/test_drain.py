import time

import pytest

import parsl
from parsl.config import Config
from parsl.executors import HighThroughputExecutor
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider

# this constant is used to scale some durations that happen
# based around the expected drain period: the drain period
# is TIME_CONST seconds, and the single executed task will
# last twice that many number of seconds.
TIME_CONST = 4

CONNECTED_MANAGERS_POLL_MS = 100


def local_config():
    return Config(
        executors=[
            HighThroughputExecutor(
                label="htex_local",
                drain_period=TIME_CONST,
                worker_debug=True,
                cores_per_worker=1,
                encrypted=True,
                provider=LocalProvider(
                    init_blocks=1,
                    min_blocks=0,
                    max_blocks=0,
                    launcher=SimpleLauncher(),
                ),
            )
        ],
        strategy='none',
        strategy_period=0.1
    )


@parsl.python_app
def f(n):
    import time
    time.sleep(n)


@pytest.mark.local
def test_drain(try_assert):

    htex = parsl.dfk().executors['htex_local']

    # wait till we have a block running...

    try_assert(lambda: len(htex.connected_managers()) == 1, check_period_ms=CONNECTED_MANAGERS_POLL_MS)

    managers = htex.connected_managers()
    assert managers[0]['active'], "The manager should be active"
    assert not managers[0]['draining'], "The manager should not be draining"

    fut = f(TIME_CONST * 2)

    time.sleep(TIME_CONST)

    # this assert should happen *very fast* after the above delay...
    try_assert(lambda: htex.connected_managers()[0]['draining'], timeout_ms=500, check_period_ms=CONNECTED_MANAGERS_POLL_MS)

    # and the test task should still be running...
    assert not fut.done(), "The test task should still be running"

    fut.result()

    # and now we should see the manager disappear...
    # ... with strategy='none', this should be coming from draining but
    # that information isn't immediately obvious from the absence in
    # connected managers.
    # As with the above draining assert, this should happen very fast after
    # the task ends.
    try_assert(lambda: len(htex.connected_managers()) == 0, timeout_ms=500, check_period_ms=CONNECTED_MANAGERS_POLL_MS)
