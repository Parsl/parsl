import threading
import time

import pytest

import parsl
from parsl.tests.configs.local_threads import fresh_config as local_config


@parsl.python_app
def slow_app(evt: threading.Event):
    evt.wait()


@pytest.mark.local
def test_wait_for_tasks():
    """
    gh#1606 reported that wait_for_current_tasks fails due to tasks being removed
    from the DFK tasks dict as they complete; bug introduced in #1543.
    """
    def test_kernel(may_wait: threading.Event):
        e1, e2 = threading.Event(), threading.Event()

        # app_slow is in *middle* of internal DFK data structure
        app_fast1, app_slow, app_fast2 = slow_app(e1), slow_app(e2), slow_app(e1)

        may_wait.set()  # initiated wait in outer test
        time.sleep(0.01)

        e1.set()

        while not all(f.done() for f in (app_fast1, app_fast2)):
            time.sleep(0.01)

        e2.set()
        app_slow.result()

    may_continue = threading.Event()
    threading.Thread(target=test_kernel, daemon=True, args=(may_continue,)).start()

    may_continue.wait()
    parsl.dfk().wait_for_current_tasks()  # per sleeps, waits for all 3 tasks
