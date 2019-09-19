import argparse
import datetime
import time

import pytest

import parsl
from parsl.app.app import python_app
from parsl.executors.high_throughput.process_worker_pool import WorkerLost

def local_setup():
    from parsl.tests.configs.htex_local import config
    config.executors[0].worker_debug = True
    config.executors[0].poll_period = 1
    config.executors[0].max_workers = 1
    parsl.load(config)


def local_teardown():
    # explicit clear without dfk.cleanup here, because the
    # test does that already
    parsl.clear()

@python_app
def kill_worker():
    import sys
    sys.exit(2)

@pytest.mark.local
def test_htex_worker_failure():
    f = kill_worker()
    with pytest.raises(WorkerLost):
        f.result()
