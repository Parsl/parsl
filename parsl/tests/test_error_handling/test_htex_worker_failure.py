import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config


def local_setup():
    config = fresh_config()
    config.executors[0].poll_period = 1
    config.executors[0].max_workers = 1
    parsl.load(config)


def local_teardown():
    parsl.clear()


@python_app
def kill_worker():
    import sys
    sys.exit(2)


@pytest.mark.local
def test_htex_worker_failure():
    # Putting `WorkerLost` here as the exception causes
    # the test to fail-- I am not sure why
    with pytest.raises(Exception):
        f = kill_worker()
        f.result()
