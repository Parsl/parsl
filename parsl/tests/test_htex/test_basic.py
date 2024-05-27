import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config


def local_setup():
    config = fresh_config()
    config.executors[0].poll_period = 1
    config.executors[0].max_workers_per_node = 1
    parsl.load(config)


def local_teardown():
    parsl.dfk().cleanup()


@python_app
def dummy():
    pass


@pytest.mark.local
def test_app():
    x = dummy()
    assert x.result() is None
