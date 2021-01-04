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
def dummy():
    pass


@pytest.mark.local
def test_that_it_fails():
    x = dummy()
    x.result()
