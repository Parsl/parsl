import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.local_threads import fresh_config


@python_app
def worker_identify(x, sleep_dur=0.2):
    import time
    import os
    import threading
    time.sleep(sleep_dur)
    return {"pid": os.getpid(),
            "tid": threading.current_thread()}


@pytest.mark.local
def test_parallel_for():
    config = fresh_config()
    dfk = parsl.load(config)
    d = []
    for i in range(0, config.executors[0].max_threads):
        d.extend([worker_identify(i)])

    [item.result() for item in d]

    thread_count = len(set([item.result()['tid'] for item in d]))
    process_count = len(set([item.result()['pid'] for item in d]))
    assert thread_count <= config.executors[0].max_threads, "More threads than allowed"
    assert process_count == 1, "More processes than allowed"
    dfk.cleanup()
    parsl.clear()
    return d
