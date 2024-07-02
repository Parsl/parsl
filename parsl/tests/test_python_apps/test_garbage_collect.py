import threading
import time

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import fresh_config as local_config


@python_app
def slow_double(x, may_continue: threading.Event):
    may_continue.wait()
    return x * 2


@pytest.mark.local
def test_garbage_collect():
    """ Launches an app with a dependency and waits till it's done and asserts that
    the internal refs were wiped
    """
    evt = threading.Event()
    x = slow_double(10, evt)
    x = slow_double(x, evt)

    assert parsl.dfk().tasks[x.tid]['app_fu'] == x, "Tasks table should have app_fu ref before done"

    evt.set()
    assert x.result() == 10 * 4
    if parsl.dfk().checkpoint_mode is not None:
        # We explicit call checkpoint if checkpoint_mode is enabled covering
        # cases like manual/periodic where checkpointing may be deferred.
        parsl.dfk().checkpoint()

    time.sleep(0.01)  # Give enough time for task wipes to work
    assert x.tid not in parsl.dfk().tasks, "Task record should be wiped after task completion"
