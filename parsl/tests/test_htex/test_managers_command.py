import logging
import sys

import pytest

import parsl
from parsl.app.app import python_app
from parsl.tests.configs.htex_local import fresh_config


def local_config():
    config = fresh_config()
    config.executors[0].poll_period = 1
    config.executors[0].max_workers_per_node = 1
    return config


@python_app
def dummy():
    pass


@pytest.mark.local
def test_connected_managers():

    # Run dummy function to ensure a manager is online
    x = dummy()
    assert x.result() is None
    executor = parsl.dfk().executors['htex_local']
    manager_info_list = executor.connected_managers()
    assert len(manager_info_list) == 1
    manager_info = manager_info_list[0]
    assert 'python_version' in manager_info
    assert 'parsl_version' in manager_info
    assert manager_info['parsl_version'] == parsl.__version__
    assert manager_info['python_version'] == f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
